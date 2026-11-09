use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;
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

fn comma_separated<'de, D>(d: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = Option::<String>::deserialize(d)?;
    Ok(s.map(|s| s.split(',').map(str::trim).filter(|c| !c.is_empty()).map(ToOwned::to_owned).collect()))
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(super) struct ObjectFilter {
    #[serde(default, deserialize_with = "comma_separated")]
    pub(super) classes: Option<Vec<String>>,
    #[serde(flatten)]
    pub(super) properties: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DateQuery {
    pub(super) timestamp: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Query;

    fn parse(query: &str) -> ObjectFilter {
        Query::<ObjectFilter>::try_from_uri(&format!("http://localhost/objects?{}", query).parse().unwrap()).unwrap().0
    }

    #[test]
    fn object_filter_parses_comma_separated_classes() {
        let f = parse("classes=A,B&color=red");
        assert_eq!(f.classes, Some(vec!["A".to_owned(), "B".to_owned()]));
        assert_eq!(f.properties.unwrap().get("color").map(String::as_str), Some("red"));

        assert_eq!(parse("classes=A").classes, Some(vec!["A".to_owned()]));
        assert_eq!(parse("").classes, None);
    }
}
