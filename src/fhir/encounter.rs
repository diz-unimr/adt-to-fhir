use crate::config::Fhir;
use crate::fhir::mapper::bundle_entry;
use fhir_model::r4b::resources::{BundleEntry, Encounter};
use hl7_parser::Message;
use std::error::Error;

pub(super) fn map_encounter(
    v2_msg: Message,
    config: Fhir,
) -> Result<Vec<BundleEntry>, Box<dyn Error>> {
    let e1 = Encounter::builder().build()?;
    let e2 = Encounter::builder().build()?;
    Ok(vec![bundle_entry(e1)?, bundle_entry(e2)?])
}
