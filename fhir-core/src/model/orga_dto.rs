use derive_builder::Builder;

#[derive(Debug, Clone, PartialEq)]
pub struct DepartmentDto {
    pub department_identifier: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WardDto {
    pub ward_name: String,
    pub part_of_department: DepartmentDto,
}

#[derive(Debug, Clone, PartialEq, Builder)]
pub struct LocationDto {
    pub bed_name: String,
    pub room: String,
    pub is_icu: bool,
    pub part_of_ward: WardDto,
}
