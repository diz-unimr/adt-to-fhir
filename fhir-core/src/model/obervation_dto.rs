use derive_builder::Builder;
use fhir_model::time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct ObservationDto {
    pub pid: String,
    pub encounter_number: String,
    pub effective_date: OffsetDateTime,
    /// if is_alive is none than no vital status observation to create
    #[builder(default)]
    pub is_alive: Option<bool>,
    #[builder(default)]
    pub head_circumference: Option<usize>,
    #[builder(default)]
    pub weight: Option<usize>,
    #[builder(default)]
    pub length: Option<usize>,
}
