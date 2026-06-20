use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use utoipa::IntoParams;

#[cfg(feature = "auth")]
pub mod auth;
#[cfg(feature = "auth")]
pub mod auth_db;
#[cfg(not(feature = "auth"))]
pub mod public;

#[derive(Debug, Deserialize)]
pub(super) struct DataFilter {
    pub(super) start: Option<DateTime<Utc>>,
    pub(super) end: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(super) struct ObjectFilter {
    pub(super) class: Option<String>,
    #[serde(flatten)]
    pub(super) extra: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DateQuery {
    pub(super) time: Option<DateTime<Utc>>,
}
