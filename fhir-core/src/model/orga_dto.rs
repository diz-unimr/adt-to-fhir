#[derive(Debug, Clone, PartialEq)]
pub struct DepartmentDto {
    pub department_identifier: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WardDto {
    pub ward_name: String,
    pub part_of_department: DepartmentDto,
}
