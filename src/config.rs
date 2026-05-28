use anyhow::anyhow;
use config::{Config, Environment, File};
use serde::Deserialize;
use validator::Validate;

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct App {
    pub(crate) log_level: String,
}

#[derive(Default, Deserialize, Clone, Debug, Validate)]
pub(crate) struct Kafka {
    pub(crate) brokers: String,
    pub(crate) security_protocol: String,
    pub(crate) ssl: Option<Ssl>,
    pub(crate) consumer_group: String,
    pub(crate) input_topic: String,
    pub(crate) output_topic: String,
    pub(crate) offset_reset: String,
    #[validate(range(min = 1, max = 20))]
    pub(crate) num_partitions: i32,
}

#[derive(Default, Deserialize, Clone)]
pub(crate) struct Fhir {
    pub(crate) facility_id: String,
    pub(crate) person: PatientConfig,
    pub(crate) fall: FallConfig,
    pub(crate) location: LocationConfig,
    pub(crate) meta_source: String,
    pub(crate) condition: SystemConfig,
    pub(crate) observation: ObservationConfig,
    pub(crate) organization: OrganizationConfig,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct PatientConfig {
    pub(crate) profile: String,
    pub(crate) system: String,
    pub(crate) other_insurance_system: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct FallConfig {
    pub(crate) profile: String,
    pub(crate) system: String,
    pub(crate) einrichtungskontakt: SystemConfig,
    pub(crate) abteilungskontakt: SystemConfig,
    pub(crate) versorgungsstellenkontakt: SystemConfig,
}
#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct LocationConfig {
    pub(crate) system_ward: String,
    pub(crate) system_room: String,
    pub(crate) system_bed: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct ObservationConfig {
    pub(crate) system: String,
    pub(crate) profile_head_circumference: String,
    pub(crate) profile_weight: String,
    pub(crate) profile_vital_status: String,
    pub(crate) profile_height: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct OrganizationConfig {
    pub(crate) department: SystemConfig,
    pub(crate) ward: SystemConfig,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct SystemConfig {
    pub(crate) system: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Ssl {
    pub(crate) ca_location: Option<String>,
    pub(crate) certificate_location: Option<String>,
    pub(crate) key_location: Option<String>,
    pub(crate) key_password: Option<String>,
}

#[derive(Default, Deserialize, Clone)]
pub(crate) struct AppConfig {
    pub(crate) app: App,
    pub(crate) kafka: Kafka,
    pub(crate) fhir: Fhir,
}

impl AppConfig {
    pub(crate) fn new() -> anyhow::Result<Self> {
        Config::builder()
            // default config from file
            .add_source(File::with_name("app.yaml"))
            // override values from environment variables
            .add_source(Environment::default().separator("."))
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
