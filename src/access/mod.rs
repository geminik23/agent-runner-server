//! Transport-independent client authentication, authorization, and dummy profiles.

mod authority;
mod request;
mod store;

pub use authority::{
    AccessError, AccessLimits, AuthenticatedClient, ClientAccessControl, ClientRegistration,
    CredentialSecret, ExecutionProfile, Scope,
};
pub(crate) use authority::{LaunchAuthorization, validate_resource_id};
pub use request::{AuthorizedStart, StartSession};

pub use store::AuthorizedStore;
