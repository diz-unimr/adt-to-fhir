use derive_builder::Builder;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct Meta {
    pub id: u64,
    pub operation: Operation,
}

impl Meta {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    UpdateAsCreate,
    CreateIfNotExists,
    Delete,
    Patch,
}

pub(crate) trait ModelDto {
    fn id(&self) -> String;
    fn operation(&self) -> Operation;
}
