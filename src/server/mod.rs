use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use utoipa::IntoParams;

#[cfg(not(feature = "secure"))]
pub mod public;
#[cfg(feature = "secure")]
pub mod secure;
#[cfg(feature = "secure")]
pub mod secure_db;

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
