use crate::fhir_error::FhirMappingError;
use crate::model::obervation_dto::ObservationDto;
use adt_config::config::Fhir;
use fhir_model::r4b::resources::BundleEntry;

pub fn map(data: &ObservationDto, config: &Fhir) -> Result<Vec<BundleEntry>, FhirMappingError> {
    let mut result = vec![];

    Ok(result)
}

fn get_vital_obs(data: &ObservationDto) -> Result<Vec<BundleEntry>, FhirMappingError> {
    todo!()
}
