use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Scenario {
    pub name: String,
    pub pattern: String,
}

#[derive(Deserialize)]
struct ScenarioFile {
    scenarios: Vec<Scenario>,
}

pub(crate) fn read_scenarios(path: &Path) -> Result<Vec<Scenario>> {
    let body = std::fs::read_to_string(path)?;
    let file: ScenarioFile = toml::from_str(&body)?;
    Ok(file.scenarios)
}

pub(crate) fn default_scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "short_literal".to_owned(),
            pattern: "needle".to_owned(),
        },
        Scenario {
            name: "long_literal".to_owned(),
            pattern: "longliteralbenchmarktoken".to_owned(),
        },
        Scenario {
            name: "alternation".to_owned(),
            pattern: "alpha|beta".to_owned(),
        },
        Scenario {
            name: "anchored".to_owned(),
            pattern: "^ANCHOR_START".to_owned(),
        },
        Scenario {
            name: "no_match".to_owned(),
            pattern: "UNMATCHABLE_TOKEN".to_owned(),
        },
        Scenario {
            name: "QAll".to_owned(),
            pattern: ".*".to_owned(),
        },
    ]
}
