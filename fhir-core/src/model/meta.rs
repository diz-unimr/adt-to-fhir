#[derive(Debug, Clone, PartialEq)]
pub struct Meta {
    pub id: u64,
    pub operation: Operation,
}

impl Meta {
    pub(crate) fn new() -> Self {
        Self {
            id: 0,
            operation: Operation::UpdateAsCreate,
        }
    }
}
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
