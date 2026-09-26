//! Transport-independent client authentication, authorization, and dummy profiles.

mod authority;
mod request;
mod store;

pub use authority::{
    AccessError, AccessLimits, AuthenticatedClient, ClientAccessControl, ClientRegistration,
    CredentialSecret, ExecutionProfile, Scope,
};
pub use request::AuthorizedStart;
pub use store::AuthorizedStore;
