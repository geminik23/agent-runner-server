use std::collections::HashSet;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;

use super::{Namespace, SkillRef, SkillWrite, SqliteStore, StorageError};

pub(crate) const MAX_SKILLS: usize = 32;
pub(crate) const COMPILER_REVISION: &str = "literal-json-v1";
pub(crate) const POLICY_REVISION: &str = "skill-policy-v1";

#[derive(Clone, Copy)]
pub(crate) struct SkillBounds {
    pub markdown: usize,
    pub metadata: usize,
    pub identifier: usize,
}

#[derive(Serialize)]
pub(crate) struct SkillRecord {
    pub skill_id: String,
    pub version: String,
    pub markdown: String,
    pub required_capabilities: Vec<String>,
    pub enabled: bool,
}

#[derive(Serialize)]
pub(crate) struct SkillSummary {
    pub skill_id: String,
    pub version: String,
    pub enabled: bool,
}

fn valid_component(value: &str, limit: usize) -> bool {
    !value.is_empty() && value.len() <= limit
}

pub(crate) fn validate_refs(
    refs: &[SkillRef],
    identifier_limit: usize,
) -> Result<(), StorageError> {
    if refs.len() > MAX_SKILLS {
        return Err(StorageError::InvalidInput("skill count"));
    }
    let mut ids = HashSet::new();
    for r in refs {
        if !valid_component(&r.skill_id, identifier_limit)
            || !valid_component(&r.version, identifier_limit)
            || !ids.insert(&r.skill_id)
        {
            return Err(StorageError::InvalidInput("skill reference"));
        }
    }
    Ok(())
}

fn record(
    connection: &Connection,
    ns: &Namespace,
    r: &SkillRef,
    bounds: SkillBounds,
) -> Result<Option<SkillRecord>, StorageError> {
    let row: Option<(Option<String>, Option<String>, bool, bool)> = connection.query_row(
        "SELECT CASE WHEN length(CAST(v.markdown AS BLOB)) <= ?5 THEN v.markdown ELSE NULL END, CASE WHEN length(CAST(COALESCE(p.required_capabilities, '[]') AS BLOB)) <= ?6 THEN COALESCE(p.required_capabilities, '[]') ELSE NULL END, COALESCE(s.enabled, 1), COALESCE(p.enabled, 1) FROM skill_versions v LEFT JOIN skills s USING (owner_id, client_id, skill_id) LEFT JOIN skill_policies p USING (owner_id, client_id, skill_id, version) WHERE v.owner_id = ?1 AND v.client_id = ?2 AND v.skill_id = ?3 AND v.version = ?4",
        params![ns.owner_id, ns.client_id, r.skill_id, r.version, i64::try_from(bounds.markdown).unwrap_or(i64::MAX), i64::try_from(bounds.metadata).unwrap_or(i64::MAX)],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).optional()?;
    row.map(|(markdown, capabilities, skill_enabled, version_enabled)| {
        let markdown = markdown.ok_or(StorageError::InvalidInput("skill limit"))?;
        let capabilities =
            capabilities.ok_or(StorageError::InvalidInput("skill metadata limit"))?;
        let required_capabilities =
            serde_json::from_str(&capabilities).map_err(|_| StorageError::InvalidState)?;
        Ok(SkillRecord {
            skill_id: r.skill_id.clone(),
            version: r.version.clone(),
            markdown,
            required_capabilities,
            enabled: skill_enabled && version_enabled,
        })
    })
    .transpose()
}

impl SqliteStore {
    pub(crate) fn register_skill(
        &mut self,
        ns: &Namespace,
        reference: &SkillRef,
        markdown: &str,
        capabilities: &[String],
        bounds: SkillBounds,
    ) -> Result<SkillWrite, StorageError> {
        let serialized = serde_json::to_string(capabilities)
            .map_err(|_| StorageError::InvalidInput("capabilities"))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO namespaces (owner_id, client_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
            params![ns.owner_id, ns.client_id],
        )?;
        if let Some(existing) = record(&tx, ns, reference, bounds)? {
            if existing.markdown.as_bytes() != markdown.as_bytes()
                || existing.required_capabilities != capabilities
            {
                return Err(StorageError::SkillVersionConflict);
            }
            tx.commit()?;
            return Ok(SkillWrite::Unchanged);
        }
        tx.execute("INSERT INTO skills (owner_id, client_id, skill_id) VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING", params![ns.owner_id, ns.client_id, reference.skill_id])?;
        tx.execute("INSERT INTO skill_versions (owner_id, client_id, skill_id, version, markdown) VALUES (?1, ?2, ?3, ?4, ?5)", params![ns.owner_id, ns.client_id, reference.skill_id, reference.version, markdown])?;
        tx.execute("INSERT INTO skill_policies (owner_id, client_id, skill_id, version, required_capabilities) VALUES (?1, ?2, ?3, ?4, ?5)", params![ns.owner_id, ns.client_id, reference.skill_id, reference.version, serialized])?;
        tx.commit()?;
        Ok(SkillWrite::Inserted)
    }

    pub(crate) fn skill(
        &self,
        ns: &Namespace,
        reference: &SkillRef,
        bounds: SkillBounds,
    ) -> Result<Option<SkillRecord>, StorageError> {
        record(&self.connection, ns, reference, bounds)
    }

    pub(crate) fn list_skills(
        &self,
        ns: &Namespace,
        after: Option<&SkillRef>,
        limit: usize,
        identifier_limit: usize,
    ) -> Result<Vec<SkillSummary>, StorageError> {
        let mut stmt = self.connection.prepare(
            "SELECT CASE WHEN length(CAST(v.skill_id AS BLOB)) <= ?6 THEN v.skill_id END, CASE WHEN length(CAST(v.version AS BLOB)) <= ?6 THEN v.version END, COALESCE(s.enabled, 1) AND COALESCE(p.enabled, 1) FROM skill_versions v LEFT JOIN skills s USING (owner_id, client_id, skill_id) LEFT JOIN skill_policies p USING (owner_id, client_id, skill_id, version) WHERE v.owner_id = ?1 AND v.client_id = ?2 AND (?3 IS NULL OR (v.skill_id, v.version) > (?3, ?4)) ORDER BY v.skill_id, v.version LIMIT ?5",
        )?;
        let rows = stmt.query_map(
            params![
                ns.owner_id,
                ns.client_id,
                after.map(|r| &r.skill_id),
                after.map(|r| &r.version),
                i64::try_from(limit).map_err(|_| StorageError::InvalidInput("limit"))?,
                i64::try_from(identifier_limit).unwrap_or(i64::MAX),
            ],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get(2)?,
                ))
            },
        )?;
        rows.map(|row| {
            let (skill_id, version, enabled) = row?;
            Ok(SkillSummary {
                skill_id: skill_id
                    .ok_or(StorageError::InvalidInput("stored skill identifier limit"))?,
                version: version
                    .ok_or(StorageError::InvalidInput("stored skill identifier limit"))?,
                enabled,
            })
        })
        .collect()
    }

    pub(crate) fn set_skill_enabled(
        &mut self,
        ns: &Namespace,
        reference: &SkillRef,
        enabled: bool,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM skill_versions WHERE owner_id = ?1 AND client_id = ?2 AND skill_id = ?3 AND version = ?4)",
            params![ns.owner_id, ns.client_id, reference.skill_id, reference.version], |row| row.get(0),
        )?;
        if !exists {
            return Err(StorageError::NotFound);
        }
        tx.execute("INSERT INTO skills (owner_id, client_id, skill_id) VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING", params![ns.owner_id, ns.client_id, reference.skill_id])?;
        tx.execute("INSERT INTO skill_policies (owner_id, client_id, skill_id, version, required_capabilities, enabled) VALUES (?1, ?2, ?3, ?4, '[]', ?5) ON CONFLICT(owner_id, client_id, skill_id, version) DO UPDATE SET enabled = excluded.enabled", params![ns.owner_id, ns.client_id, reference.skill_id, reference.version, enabled])?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn skill_defaults(
        &self,
        ns: &Namespace,
        bounds: SkillBounds,
    ) -> Result<Vec<SkillRef>, StorageError> {
        defaults(&self.connection, ns, bounds)
    }

    pub(crate) fn set_skill_defaults(
        &mut self,
        ns: &Namespace,
        refs: &[SkillRef],
        bounds: SkillBounds,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO namespaces (owner_id, client_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
            params![ns.owner_id, ns.client_id],
        )?;
        for r in refs {
            let skill = record(&tx, ns, r, bounds)?.ok_or(StorageError::NotFound)?;
            if !skill.enabled {
                return Err(StorageError::SkillUnavailable);
            }
        }
        tx.execute(
            "DELETE FROM skill_defaults WHERE owner_id = ?1 AND client_id = ?2",
            params![ns.owner_id, ns.client_id],
        )?;
        for (position, r) in refs.iter().enumerate() {
            tx.execute("INSERT INTO skill_defaults (owner_id, client_id, position, skill_id, version) VALUES (?1, ?2, ?3, ?4, ?5)", params![ns.owner_id, ns.client_id, position as i64, r.skill_id, r.version])?;
        }
        tx.commit()?;
        Ok(())
    }
}

fn defaults(
    connection: &Connection,
    ns: &Namespace,
    bounds: SkillBounds,
) -> Result<Vec<SkillRef>, StorageError> {
    let mut stmt = connection.prepare("SELECT CASE WHEN length(CAST(skill_id AS BLOB)) <= ?3 THEN skill_id END, CASE WHEN length(CAST(version AS BLOB)) <= ?3 THEN version END FROM skill_defaults WHERE owner_id = ?1 AND client_id = ?2 ORDER BY position LIMIT 33")?;
    let rows = stmt.query_map(
        params![
            ns.owner_id,
            ns.client_id,
            i64::try_from(bounds.identifier).unwrap_or(i64::MAX)
        ],
        |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                r.get::<_, Option<String>>(1)?,
            ))
        },
    )?;
    let mut result = Vec::new();
    for row in rows {
        let (id, version) = row?;
        result.push(SkillRef {
            skill_id: id.ok_or(StorageError::InvalidInput("stored skill identifier limit"))?,
            version: version.ok_or(StorageError::InvalidInput("stored skill identifier limit"))?,
        });
        if result.len() > MAX_SKILLS {
            return Err(StorageError::InvalidInput("skill count"));
        }
    }
    Ok(result)
}

pub(crate) fn resolve(
    tx: &Transaction<'_>,
    ns: &Namespace,
    requested: Option<&[SkillRef]>,
    may_use_skills: bool,
    bounds: SkillBounds,
) -> Result<Vec<SkillRef>, StorageError> {
    let selected = match requested {
        Some(refs) => refs.to_vec(),
        None => defaults(tx, ns, bounds)?,
    };
    if selected.len() > MAX_SKILLS {
        return Err(StorageError::InvalidInput("skill count"));
    }
    if !selected.is_empty() && !may_use_skills {
        return Err(StorageError::SkillUseForbidden);
    }
    for r in &selected {
        let skill = record(tx, ns, r, bounds)?.ok_or(StorageError::SkillUnavailable)?;
        if !skill.enabled
            || skill
                .required_capabilities
                .iter()
                .any(|c| c != "literal_prompt")
        {
            return Err(StorageError::SkillUnavailable);
        }
    }
    Ok(selected)
}

pub(crate) fn bundle(
    connection: &Connection,
    table: &str,
    ns: &Namespace,
    id: &str,
    bounds: SkillBounds,
) -> Result<Vec<SkillRef>, StorageError> {
    let sql = match table {
        "run" => {
            "SELECT CASE WHEN length(CAST(skill_id AS BLOB)) <= ?4 THEN skill_id END, CASE WHEN length(CAST(version AS BLOB)) <= ?4 THEN version END FROM run_skills WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 ORDER BY position, skill_id LIMIT 33"
        }
        "session" => {
            "SELECT CASE WHEN length(CAST(skill_id AS BLOB)) <= ?4 THEN skill_id END, CASE WHEN length(CAST(version AS BLOB)) <= ?4 THEN version END FROM session_skills WHERE owner_id = ?1 AND client_id = ?2 AND session_id = ?3 ORDER BY position LIMIT 33"
        }
        _ => return Err(StorageError::InvalidState),
    };
    let mut stmt = connection.prepare(sql)?;
    let rows = stmt.query_map(
        params![
            ns.owner_id,
            ns.client_id,
            id,
            i64::try_from(bounds.identifier).unwrap_or(i64::MAX)
        ],
        |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                r.get::<_, Option<String>>(1)?,
            ))
        },
    )?;
    let mut result = Vec::new();
    for row in rows {
        let (skill_id, version) = row?;
        result.push(SkillRef {
            skill_id: skill_id
                .ok_or(StorageError::InvalidInput("stored skill identifier limit"))?,
            version: version.ok_or(StorageError::InvalidInput("stored skill identifier limit"))?,
        });
        if result.len() > MAX_SKILLS {
            return Err(StorageError::InvalidInput("skill count"));
        }
    }
    Ok(result)
}

pub(crate) fn compile(
    tx: &Transaction<'_>,
    ns: &Namespace,
    run_id: &str,
    input: &str,
    max_bytes: usize,
    bounds: SkillBounds,
) -> Result<String, StorageError> {
    let refs = bundle(tx, "run", ns, run_id, bounds)?;
    if refs.is_empty() {
        if input.len() > max_bytes {
            return Err(StorageError::InvalidInput("prompt limit"));
        }
        return Ok(input.to_owned());
    }
    let mut prompt = String::new();
    for r in &refs {
        let skill = record(tx, ns, r, bounds)?.ok_or(StorageError::SkillUnavailable)?;
        if !skill.enabled
            || skill
                .required_capabilities
                .iter()
                .any(|c| c != "literal_prompt")
        {
            return Err(StorageError::SkillUnavailable);
        }
        prompt.push_str("[Skill: ");
        prompt.push_str(&serde_json::to_string(r).map_err(|_| StorageError::InvalidState)?);
        prompt.push_str("]\n");
        prompt.push_str(&skill.markdown);
        prompt.push_str("\n[/Skill]\n\n");
        if prompt.len() > max_bytes {
            return Err(StorageError::InvalidInput("prompt limit"));
        }
    }
    prompt.push_str("[User request]\n");
    prompt.push_str(input);
    if prompt.len() > max_bytes {
        return Err(StorageError::InvalidInput("prompt limit"));
    }
    Ok(prompt)
}
