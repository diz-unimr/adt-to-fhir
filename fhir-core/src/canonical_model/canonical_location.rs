use derive_builder::Builder;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct LocationCdm {
    pub name: String,
    pub manging_organization: Option<String>,
    pub is_icu: bool,
    pub typ: LocationCdmType,
    /// at ambulatory encounter this is per convention `true`
    pub is_movement_finished: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LocationCdmType {
    Ward,
    Room,
    Bed,
}
