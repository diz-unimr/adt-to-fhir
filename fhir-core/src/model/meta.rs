use derive_builder::Builder;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct MappingOpPerson {
    pub id: String,
    pub operation: MappingTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingOperation {
    UpdateAsCreate,
    CreateIfNotExists,
    Delete,
    Patch,
    Skip,
}
#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct MappingOpEncounter {
    pub id: String,
    pub operation_lv1: ProcessingOperation,
    pub operation_lv2: ProcessingOperation,
    pub operation_lv3: ProcessingOperation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappingTarget {
    Person(ProcessingOperation),
    Observation(ProcessingOperation),
    Observation_with_Zng(ProcessingOperation),
}
