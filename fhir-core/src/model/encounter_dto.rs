use crate::model::meta::MappingOpEncounter;
use derive_builder::Builder;
use fhir_model::time::OffsetDateTime;
use std::num::NonZeroU32;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct Fall_Diagnose {
    pub ordinal_number: u32,
    pub id: String,
    pub condition_typ: String,
    pub priority: NonZeroU32,
}

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct Fall {
    pub meta: MappingOpEncounter,

    pub pid: String,

    pub visit_number: String,

    pub bed_status: String,

    pub admission_datetime: OffsetDateTime,
    #[builder(default)]
    pub admission_type: Option<String>,

    #[builder(default)]
    pub admission_reason_1_2: Option<String>,
    #[builder(default)]
    pub admission_reason_3_4: Option<String>,

    pub movement_id: String,

    pub movement_start: OffsetDateTime,
    #[builder(default)]
    pub movement_end: Option<OffsetDateTime>,

    #[builder(default)]
    pub discharge: Option<OffsetDateTime>,
    #[builder(default)]
    pub discharge_reason_12: Option<String>,
    #[builder(default)]
    pub discharge_reason_3: Option<String>,
    #[builder(default)]
    pub diagnosis: Option<Vec<Fall_Diagnose>>,
    #[builder(default)]
    pub mothers_enc_number: Option<String>,
    #[builder(default)]
    pub ward_short_name: Option<String>,
}
impl Fall {
    pub fn id(&self) -> &String {
        &self.meta.id
    }
}
