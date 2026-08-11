use crate::model::meta::{Meta, Operation};
use chrono::NaiveDate;

impl crate::model::meta::ModelDto for PersonDto {
    fn id(&self) -> String {
        if let Some(pid) = &self.pid {
            pid.to_string()
        } else {
            String::new()
        }
    }

    fn operation(&self) -> Operation {
        self.meta.operation
    }
}

/// Main structure for person data
#[derive(Debug, Clone, PartialEq)]
pub struct PersonDto {
    pub meta: Meta,
    pub pid: Option<String>,
    pub last_name: Option<String>,
    pub name_prefix: Option<String>,
    pub name_suffix: Option<String>,
    pub maiden_name: Option<String>,
    pub first_names: Option<String>,
    pub title: Option<String>,
    pub gender: Option<GenderDto>,
    pub date_of_birth: Option<NaiveDate>,
    pub marital_status: Option<MaritalStatusDto>,
    pub nationality: Option<String>,
    pub is_multiple_birth: Option<bool>,
    pub multiple_birth_order: Option<u32>,
    pub mother_case_number_birth: Option<String>,
    pub is_deceased_indicator: Option<bool>,
    pub occupation: Option<String>,
    pub time_of_death: Option<NaiveDate>,
    pub replaced_by_pid: Option<String>,
    pub address: Vec<Option<AddressDto>>,
}

/// Gender as enum for type-safe processing
#[derive(Debug, Clone, PartialEq)]
pub enum GenderDto {
    Male,
    Female,
    Diverse,
    Unknown,
}

impl GenderDto {
    /// Converts HL7 code (M, F, O, U) to Gender
    pub fn from_hl7(code: &str) -> Option<Self> {
        match code.to_uppercase().as_str() {
            "M" => Some(GenderDto::Male),
            "F" => Some(GenderDto::Female),
            "O" => Some(GenderDto::Diverse),
            "U" | "UNK" => Some(GenderDto::Unknown),
            _ => None,
        }
    }

    /// Converts to HL7 code
    pub fn to_hl7(&self) -> &str {
        match self {
            GenderDto::Male => "M",
            GenderDto::Female => "F",
            GenderDto::Diverse => "O",
            GenderDto::Unknown => "U",
        }
    }
}

/// Marital status based on HL7 v3 MaritalStatus code system
///
/// Mapping based on HL7 PID-16 field:
/// - A/E → Legally Separated (L)
/// - D → Divorced (D)
/// - M → Married (M)
/// - S → Never Married (S)
/// - W → Widowed (W)
/// - C → Common Law (C)
/// - G/P/R → Domestic Partner (T)
/// - N → Annulled (A)
/// - I → Interlocutory (I)
/// - B → Unmarried (U)
/// - All others → Unknown (UNK)
#[derive(Debug, Clone, PartialEq)]
pub enum MaritalStatusDto {
    /// Annulled (HL7: N, v3: A)
    Annulled,
    /// Common Law (HL7: C, v3: C)
    CommonLaw,
    /// Divorced (HL7: D, v3: D)
    Divorced,
    /// Domestic Partner (HL7: G/P/R, v3: T)
    DomesticPartner,
    /// Interlocutory (HL7: I, v3: I)
    Interlocutory,
    /// Legally Separated (HL7: A/E, v3: L)
    LegallySeparated,
    /// Married (HL7: M, v3: M)
    Married,
    /// Never Married (HL7: S, v3: S)
    NeverMarried,
    /// Unmarried (HL7: B, v3: U)
    Unmarried,
    /// Widowed (HL7: W, v3: W)
    Widowed,
    /// Unknown/Other (v3: UNK)
    Unknown,
}

impl MaritalStatusDto {
    /// Converts HL7 code to MaritalStatus based on the provided mapping logic
    pub fn from_hl7(code: &str) -> Self {
        match code.to_uppercase().as_str() {
            "A" | "E" => MaritalStatusDto::LegallySeparated,
            "D" => MaritalStatusDto::Divorced,
            "M" => MaritalStatusDto::Married,
            "S" => MaritalStatusDto::NeverMarried,
            "W" => MaritalStatusDto::Widowed,
            "C" => MaritalStatusDto::CommonLaw,
            "G" | "P" | "R" => MaritalStatusDto::DomesticPartner,
            "N" => MaritalStatusDto::Annulled,
            "I" => MaritalStatusDto::Interlocutory,
            "B" => MaritalStatusDto::Unmarried,
            _ => MaritalStatusDto::Unknown,
        }
    }

    /// Returns the HL7 input code(s) that map to this status
    pub fn to_hl7_codes(&self) -> Vec<&'static str> {
        match self {
            MaritalStatusDto::LegallySeparated => vec!["A", "E"],
            MaritalStatusDto::Divorced => vec!["D"],
            MaritalStatusDto::Married => vec!["M"],
            MaritalStatusDto::NeverMarried => vec!["S"],
            MaritalStatusDto::Widowed => vec!["W"],
            MaritalStatusDto::CommonLaw => vec!["C"],
            MaritalStatusDto::DomesticPartner => vec!["G", "P", "R"],
            MaritalStatusDto::Annulled => vec!["N"],
            MaritalStatusDto::Interlocutory => vec!["I"],
            MaritalStatusDto::Unmarried => vec!["B"],
            MaritalStatusDto::Unknown => vec!["UNK"],
        }
    }

    /// Returns the HL7 v3 MaritalStatus code
    pub fn to_v3_code(&self) -> &'static str {
        match self {
            MaritalStatusDto::Annulled => "A",
            MaritalStatusDto::CommonLaw => "C",
            MaritalStatusDto::Divorced => "D",
            MaritalStatusDto::DomesticPartner => "T",
            MaritalStatusDto::Interlocutory => "I",
            MaritalStatusDto::LegallySeparated => "L",
            MaritalStatusDto::Married => "M",
            MaritalStatusDto::NeverMarried => "S",
            MaritalStatusDto::Unmarried => "U",
            MaritalStatusDto::Widowed => "W",
            MaritalStatusDto::Unknown => "UNK",
        }
    }

    /// Returns the display name for the status
    pub fn display_name(&self) -> &'static str {
        match self {
            MaritalStatusDto::Annulled => "Annulled",
            MaritalStatusDto::CommonLaw => "Common Law",
            MaritalStatusDto::Divorced => "Divorced",
            MaritalStatusDto::DomesticPartner => "Domestic partner",
            MaritalStatusDto::Interlocutory => "Interlocutory",
            MaritalStatusDto::LegallySeparated => "Legally Separated",
            MaritalStatusDto::Married => "Married",
            MaritalStatusDto::NeverMarried => "Never Married",
            MaritalStatusDto::Unmarried => "Unmarried",
            MaritalStatusDto::Widowed => "Widowed",
            MaritalStatusDto::Unknown => "Unknown",
        }
    }

    /// Returns the HL7 v3 code system URL
    pub fn code_system_url(&self) -> &'static str {
        match self {
            MaritalStatusDto::Unknown => "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            _ => "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
        }
    }

    /// Creates a Coding representation (similar to the match statement in the original code)
    pub fn to_coding(&self) -> (String, String, String) {
        (
            self.code_system_url().to_string(),
            self.to_v3_code().to_string(),
            self.display_name().to_string(),
        )
    }
}

/// Address structure
#[derive(Debug, Clone, PartialEq)]
pub struct AddressDto {
    pub street_and_number: Vec<Option<String>>,
    pub city: Option<String>,
    pub zip_code: Option<String>,
    pub country: Option<String>,
}

impl AddressDto {
    pub fn new() -> Self {
        AddressDto {
            street_and_number: vec![None],
            city: None,
            zip_code: None,
            country: None,
        }
    }
}

impl Default for AddressDto {
    fn default() -> Self {
        Self::new()
    }
}

impl PersonDto {
    /// Creates a new empty Person
    pub fn new() -> Self {
        PersonDto {
            meta: Meta::new(),
            pid: None,
            last_name: None,
            name_prefix: None,
            name_suffix: None,
            maiden_name: None,
            first_names: None,
            title: None,
            gender: None,
            date_of_birth: None,
            marital_status: None,
            nationality: None,
            is_multiple_birth: None,
            multiple_birth_order: None,
            mother_case_number_birth: None,
            is_deceased_indicator: None,
            occupation: None,
            time_of_death: None,
            replaced_by_pid: None,
            address: vec![None],
        }
    }

    /// Checks if the person is valid (all relevant fields filled)
    pub fn is_valid(&self) -> bool {
        self.pid.is_some()
            && self.last_name.is_some()
            && self.first_names.is_some()
            && self.date_of_birth.is_some()
    }

    /// Returns the full name
    pub fn full_name(&self) -> String {
        let mut name = String::new();

        if let Some(ref title) = self.title {
            name.push_str(title);
            name.push(' ');
        }

        if let Some(ref first_name) = self.first_names {
            name.push_str(first_name);
            name.push(' ');
        }

        if let Some(ref last_name) = self.last_name {
            name.push_str(last_name);
        }

        if let Some(ref suffix) = self.name_suffix {
            name.push(' ');
            name.push_str(suffix);
        }

        name.trim().to_string()
    }
}

impl Default for PersonDto {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_person() {
        let person = PersonDto {
            pid: Some("12345".to_string()),
            last_name: Some("Mustermann".to_string()),
            first_names: Some("Max".to_string()),
            date_of_birth: Some(NaiveDate::from_ymd_opt(1990, 1, 15).unwrap()),
            gender: Some(GenderDto::Male),
            ..PersonDto::new()
        };

        assert_eq!(person.full_name(), "Max Mustermann");
        assert!(person.is_valid());
    }

    #[test]
    fn test_gender_conversion() {
        assert_eq!(GenderDto::from_hl7("M"), Some(GenderDto::Male));
        assert_eq!(GenderDto::from_hl7("F"), Some(GenderDto::Female));
        assert_eq!(GenderDto::from_hl7("O"), Some(GenderDto::Diverse));
        assert_eq!(GenderDto::from_hl7("X"), None);
    }

    #[test]
    fn test_marital_status_from_hl7() {
        // Test all mappings from the original match statement
        assert_eq!(
            MaritalStatusDto::from_hl7("A"),
            MaritalStatusDto::LegallySeparated
        );
        assert_eq!(
            MaritalStatusDto::from_hl7("E"),
            MaritalStatusDto::LegallySeparated
        );
        assert_eq!(MaritalStatusDto::from_hl7("D"), MaritalStatusDto::Divorced);
        assert_eq!(MaritalStatusDto::from_hl7("M"), MaritalStatusDto::Married);
        assert_eq!(
            MaritalStatusDto::from_hl7("S"),
            MaritalStatusDto::NeverMarried
        );
        assert_eq!(MaritalStatusDto::from_hl7("W"), MaritalStatusDto::Widowed);
        assert_eq!(MaritalStatusDto::from_hl7("C"), MaritalStatusDto::CommonLaw);
        assert_eq!(
            MaritalStatusDto::from_hl7("G"),
            MaritalStatusDto::DomesticPartner
        );
        assert_eq!(
            MaritalStatusDto::from_hl7("P"),
            MaritalStatusDto::DomesticPartner
        );
        assert_eq!(
            MaritalStatusDto::from_hl7("R"),
            MaritalStatusDto::DomesticPartner
        );
        assert_eq!(MaritalStatusDto::from_hl7("N"), MaritalStatusDto::Annulled);
        assert_eq!(
            MaritalStatusDto::from_hl7("I"),
            MaritalStatusDto::Interlocutory
        );
        assert_eq!(MaritalStatusDto::from_hl7("B"), MaritalStatusDto::Unmarried);
        assert_eq!(MaritalStatusDto::from_hl7("X"), MaritalStatusDto::Unknown);
        assert_eq!(MaritalStatusDto::from_hl7(""), MaritalStatusDto::Unknown);
    }

    #[test]
    fn test_marital_status_v3_codes() {
        assert_eq!(MaritalStatusDto::LegallySeparated.to_v3_code(), "L");
        assert_eq!(MaritalStatusDto::Divorced.to_v3_code(), "D");
        assert_eq!(MaritalStatusDto::Married.to_v3_code(), "M");
        assert_eq!(MaritalStatusDto::NeverMarried.to_v3_code(), "S");
        assert_eq!(MaritalStatusDto::Widowed.to_v3_code(), "W");
        assert_eq!(MaritalStatusDto::CommonLaw.to_v3_code(), "C");
        assert_eq!(MaritalStatusDto::DomesticPartner.to_v3_code(), "T");
        assert_eq!(MaritalStatusDto::Annulled.to_v3_code(), "A");
        assert_eq!(MaritalStatusDto::Interlocutory.to_v3_code(), "I");
        assert_eq!(MaritalStatusDto::Unmarried.to_v3_code(), "U");
        assert_eq!(MaritalStatusDto::Unknown.to_v3_code(), "UNK");
    }

    #[test]
    fn test_marital_status_display_names() {
        assert_eq!(
            MaritalStatusDto::LegallySeparated.display_name(),
            "Legally Separated"
        );
        assert_eq!(
            MaritalStatusDto::DomesticPartner.display_name(),
            "Domestic partner"
        );
        assert_eq!(
            MaritalStatusDto::NeverMarried.display_name(),
            "Never Married"
        );
    }

    #[test]
    fn test_marital_status_coding() {
        let (system, code, display) = MaritalStatusDto::Married.to_coding();
        assert_eq!(
            system,
            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"
        );
        assert_eq!(code, "M");
        assert_eq!(display, "Married");

        let (system, code, display) = MaritalStatusDto::Unknown.to_coding();
        assert_eq!(
            system,
            "http://terminology.hl7.org/CodeSystem/v3-NullFlavor"
        );
        assert_eq!(code, "UNK");
        assert_eq!(display, "Unknown");
    }

    #[test]
    fn test_address() {
        let address = AddressDto {
            street_and_number: vec![Some("Main Street 42".to_string())],
            city: Some("Sample City".to_string()),
            zip_code: Some("12345".to_string()),
            country: Some("Germany".to_string()),
        };

        let person = PersonDto {
            address: vec![Some(address)],
            ..PersonDto::new()
        };

        assert!(!person.address.is_empty());
        assert_eq!(
            person
                .address
                .first()
                .unwrap()
                .as_ref()
                .unwrap()
                .city
                .as_deref(),
            Some("Sample City")
        );
    }

    #[test]
    fn test_full_name_with_title_and_suffix() {
        let person = PersonDto {
            title: Some("Dr.".to_string()),
            first_names: Some("Max".to_string()),
            last_name: Some("Mustermann".to_string()),
            name_suffix: Some("Jr.".to_string()),
            ..PersonDto::new()
        };

        assert_eq!(person.full_name(), "Dr. Max Mustermann Jr.");
    }

    #[test]
    fn test_invalid_person() {
        let person = PersonDto::new();
        assert!(!person.is_valid());
    }
}
