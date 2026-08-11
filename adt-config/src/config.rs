use anyhow::anyhow;
use config::{Config, Environment, File};
use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Default, Debug, Deserialize, Clone)]
pub struct App {
    pub log_level: String,
    pub telemetry_endpoint: String,
}

#[derive(Default, Deserialize, Clone, Debug, Validate)]
pub struct Kafka {
    pub brokers: String,
    pub security_protocol: String,
    pub ssl: Option<Ssl>,
    pub consumer_group: String,
    pub input_topic: String,
    pub output_topic: String,
    pub offset_reset: String,
    #[validate(range(min = 1, max = 20))]
    pub num_partitions: i32,
}

#[derive(Deserialize, Clone)]
pub struct Fhir {
    pub check_mode: CheckMode,
    pub facility_id: String,
    pub bundle_identifier_system: String,
    pub person: PatientConfig,
    pub fall: FallConfig,
    pub location: LocationConfig,
    pub meta_source: String,
    pub condition: SystemConfig,
    pub observation: ObservationConfig,
    pub organization: OrganizationConfig,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub struct PatientConfig {
    pub profile: String,
    pub system: String,
    pub other_insurance_system: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub struct FallConfig {
    pub profile: String,
    pub system: String,
    pub einrichtungskontakt: SystemConfig,
    pub abteilungskontakt: SystemConfig,
    pub versorgungsstellenkontakt: SystemConfig,
}
#[derive(Default, Debug, Deserialize, Clone)]
pub struct LocationConfig {
    pub system_ward: String,
    pub system_room: String,
    pub system_bed: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub struct ObservationConfig {
    pub system: String,
    pub profile_head_circumference: String,
    pub profile_weight: String,
    pub profile_vital_status: String,
    pub profile_height: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub struct OrganizationConfig {
    pub department: SystemConfig,
    pub ward: SystemConfig,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub struct SystemConfig {
    pub system: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub struct Ssl {
    pub ca_location: Option<String>,
    pub certificate_location: Option<String>,
    pub key_location: Option<String>,
    pub key_password: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct AppConfig {
    pub app: App,
    pub kafka: Kafka,
    pub fhir: Fhir,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckMode {
    Strict,
    Lenient,
}

impl AppConfig {
    pub fn new() -> anyhow::Result<Self> {
        Self::with_env(Environment::default().separator("."))
    }
    fn with_env(env: Environment) -> anyhow::Result<Self> {
        Config::builder()
            // default config from file
            .add_source(File::with_name("app.yaml"))
            // override values from environment variables
            .add_source(env)
            .build()?
            // .map_err(|e| anyhow!(e))
            .try_deserialize::<Self>()
            // validate
            .map(|c| match c.kafka.validate() {
                Ok(()) => Ok(c),
                Err(e) => Err(anyhow!(e)),
            })?
    }
}

#[cfg(test)]
mod tests {
    use crate::config::AppConfig;
    use config::Environment;
    use std::collections::HashMap;

    #[test]
    fn default_config_validates() {
        match AppConfig::new() {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e)
            }
        }
    }

    #[test]
    fn invalid_config_fails() {
        // override validated property with invalid data
        let source = Environment::default().source(Some({
            let mut env = HashMap::new();
            env.insert("kafka.num_partitions".into(), "0".into());
            env
        }));

        let c = AppConfig::with_env(source);

        assert!(c.is_err());
    }
}
