use crate::canonical_model::canonical_organization::WardCdm;
use derive_builder::Builder;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct LocationCdm {
    /// usually build from station name and w
    pub ward_short_name: String,
    /// at ambulatory encounter this is per convention `true`
    pub is_movement_finished: bool,
    /// is derived from visit date and resource entries provided at configuration
    pub is_isu_location: bool,
    /// not applicable to ambulatory encounter therefore optional
    #[builder(default)]
    pub room: Option<String>,
    /// not applicable to ambulatory encounter therefore optional
    #[builder(default)]
    pub bed: Option<String>,

    pub part_of_ward: WardCdm,
}
