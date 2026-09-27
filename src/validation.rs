use std::{collections::HashSet, fmt};

use serde::{
    Deserialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value};

const MAX_SCHEMA_DEPTH: usize = 12;
const MAX_SCHEMA_NODES: usize = 128;
pub(crate) const MAX_RESULT_VALIDATION_BYTES: usize = 256 * 1024;
pub(crate) const MIN_VALIDATED_RESULT_BYTES: usize = 192;

pub(crate) fn parse_unique_json(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    serde_json::from_slice::<UniqueValue>(bytes).map(|value| value.0)
}

struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("JSON without duplicate keys")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut result = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if result.contains_key(&key) {
                        return Err(de::Error::custom("duplicate key"));
                    }
                    result.insert(key, map.next_value::<UniqueValue>()?.0);
                }
                Ok(UniqueValue(Value::Object(result)))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<UniqueValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value.into())))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value)))
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|number| UniqueValue(Value::Number(number)))
                    .ok_or_else(|| de::Error::custom("invalid number"))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

/// Checks the explicitly supported schema subset without evaluating output or resolving references.
pub(crate) fn valid_schema(schema: &Value) -> bool {
    fn walk(schema: &Value, depth: usize, nodes: &mut usize) -> bool {
        *nodes += 1;
        if depth > MAX_SCHEMA_DEPTH || *nodes > MAX_SCHEMA_NODES {
            return false;
        }
        let Some(object) = schema.as_object() else {
            return false;
        };
        let Some(kind) = object.get("type").and_then(Value::as_str) else {
            return false;
        };
        if !["object", "array", "string", "number", "boolean", "null"].contains(&kind) {
            return false;
        }
        if object.keys().any(|key| {
            ![
                "type",
                "properties",
                "required",
                "additionalProperties",
                "items",
            ]
            .contains(&key.as_str())
        }) {
            return false;
        }
        if kind != "object"
            && (object.contains_key("properties")
                || object.contains_key("required")
                || object.contains_key("additionalProperties"))
        {
            return false;
        }
        if kind != "array" && object.contains_key("items") {
            return false;
        }
        if let Some(properties) = object.get("properties") {
            let Some(properties) = properties.as_object() else {
                return false;
            };
            for child in properties.values() {
                if !walk(child, depth + 1, nodes) {
                    return false;
                }
            }
        }
        if let Some(required) = object.get("required") {
            let Some(required) = required.as_array() else {
                return false;
            };
            let mut names = HashSet::new();
            if required.len() > MAX_SCHEMA_NODES
                || required
                    .iter()
                    .any(|item| item.as_str().is_none_or(|name| !names.insert(name)))
            {
                return false;
            }
        }
        if object
            .get("additionalProperties")
            .is_some_and(|value| !value.is_boolean())
        {
            return false;
        }
        if let Some(items) = object.get("items")
            && !walk(items, depth + 1, nodes)
        {
            return false;
        }
        true
    }
    walk(schema, 0, &mut 0)
}

/// Validates a parsed result against a schema already accepted by `valid_schema`.
pub(crate) fn matches_schema(schema: &Value, value: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    let Some(kind) = object.get("type").and_then(Value::as_str) else {
        return false;
    };
    match kind {
        "object" => {
            let Some(value) = value.as_object() else {
                return false;
            };
            if let Some(required) = object.get("required").and_then(Value::as_array)
                && required
                    .iter()
                    .any(|key| key.as_str().is_none_or(|key| !value.contains_key(key)))
            {
                return false;
            }
            let properties = object.get("properties").and_then(Value::as_object);
            for (key, item) in value {
                if let Some(child) = properties.and_then(|properties| properties.get(key)) {
                    if !matches_schema(child, item) {
                        return false;
                    }
                } else if object.get("additionalProperties").and_then(Value::as_bool) == Some(false)
                {
                    return false;
                }
            }
            true
        }
        "array" => value.as_array().is_some_and(|items| {
            object
                .get("items")
                .is_none_or(|schema| items.iter().all(|item| matches_schema(schema, item)))
        }),
        "string" => value.is_string(),
        "number" => value.is_number(),

        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn schema_checks_nested_required_properties_and_array_items() {
        let schema = json!({"type":"object","required":["items"],"additionalProperties":false,"properties":{"items":{"type":"array","items":{"type":"number"}}}});
        assert!(valid_schema(&schema));
        assert!(matches_schema(&schema, &json!({"items":[1,2.0]})));
        assert!(!matches_schema(&schema, &json!({"items":[1,"bad"]})));
        assert!(!matches_schema(&schema, &json!({"items":[],"extra":true})));
        assert!(!matches_schema(&schema, &json!({})));
    }

    #[test]
    fn unsupported_or_ambiguous_schemas_and_duplicate_json_are_rejected() {
        for schema in [
            json!({"type":"object","$ref":"https://example.invalid"}),
            json!({"type":"string","required":["x"]}),
            json!({"type":"object","required":["x","x"]}),
            json!({"type":["string","null"]}),
            json!({"type":"object","additionalProperties":null}),
            json!({"type":"integer"}),
        ] {
            assert!(!valid_schema(&schema), "{schema}");
        }
        let mut too_deep = json!({"type":"string"});
        for _ in 0..14 {
            too_deep = json!({"type":"array","items":too_deep});
        }
        assert!(!valid_schema(&too_deep));
        let mut properties = serde_json::Map::new();
        for index in 0..129 {
            properties.insert(format!("key{index}"), json!({"type":"string"}));
        }
        assert!(!valid_schema(
            &json!({"type":"object","properties":properties})
        ));
        assert!(parse_unique_json(br#"{"value":1,"value":2}"#).is_err());
        assert!(parse_unique_json(br#"{"nested":{"value":1,"value":2}}"#).is_err());
    }
}
