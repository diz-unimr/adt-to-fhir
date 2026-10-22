use crate::canonical_model::canonical_location::LocationCdm;
use crate::canonical_model::canonical_meta::MappingOpEncounter;
use derive_builder::Builder;
use fhir_model::time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct EncounterDiagnosisCdm {
    pub ordinal_number: u32,
    pub id: String,
    pub condition_typ: String,
    pub priority: String,
}

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct EncounterCdm {
    pub meta: MappingOpEncounter,
    pub pid: String,
    pub visit_number: String,
    pub bed_status: String,
    pub admission_datetime: OffsetDateTime,
    #[builder(default)]
    pub admission_type: Option<AdmissionType>,
    /// admission reason §301 first and second digit
    #[builder(default)]
    pub admission_reason_1_2: Option<String>,
    /// admission reason §301 third and fourth digit
    #[builder(default)]
    pub admission_reason_3_4: Option<String>,
    pub movement_id: String,
    pub movement_start: OffsetDateTime,
    #[builder(default)]
    pub movement_end: Option<OffsetDateTime>,
    #[builder(default)]
    pub discharge_date: Option<OffsetDateTime>,
    #[builder(default)]
    pub discharge_reason_12: Option<String>,
    #[builder(default)]
    pub discharge_reason_3: Option<String>,
    #[builder(default)]
    pub diagnosis: Option<Vec<EncounterDiagnosisCdm>>,
    #[builder(default)]
    pub mothers_enc_number: Option<String>,
    pub ward_short_name: Option<String>,
    pub department_long_name: Option<String>,
    pub department_short_name: Option<String>,
    pub clinical_department_code: Option<String>,

    pub is_icu_location: bool,
    /// necessary for detailed HL7 A04 stream data mapping
    pub is_registration: bool,

    pub locations: Option<Vec<LocationCdm>>,
}
impl EncounterCdm {
    pub fn id(&self) -> &String {
        &self.meta.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionType {
    /// 'A'
    AccidentAmbulance,
    /// without hospitalization briefing ('E')
    Emergency,
    /// 'R'
    Normal,
    /// 'U'
    UrgentAdmission,
    /// 'C'
    Chosen,
}
