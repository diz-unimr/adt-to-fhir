use derive_builder::Builder;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct MappingOp {
    pub id: String,
    pub operation: Operation,
}

impl MappingOp {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    UpdateAsCreate,
    CreateIfNotExists,
    Delete,
    Patch,
    Skip,
}

pub(crate) trait ModelDto {
    fn id(&self) -> String;
    fn operation(&self) -> Operation;
}
