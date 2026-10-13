#[derive(Debug, Clone, PartialEq)]
pub struct DepartmentCdm {
    pub department_identifier: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WardCdm {
    pub ward_name: String,
    pub part_of_department: DepartmentCdm,
}
