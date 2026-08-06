use chrono::NaiveDate;
use serde::Deserialize;

/// Main structure for person data
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Person {
    pub pid: Option<String>,
    pub last_name: Option<String>,
    pub name_prefix: Option<String>,
    pub name_suffix: Option<String>,
    pub maiden_name: Option<String>,
    pub first_names: Option<String>,
    pub title: Option<String>,
    pub gender: Option<Gender>,
    pub date_of_birth: Option<NaiveDate>,
    pub marital_status: Option<MaritalStatus>,
    pub nationality: Option<String>,
    pub is_multiple_birth: Option<bool>,
    pub multiple_birth_order: Option<u32>,
    pub mother_case_number_birth: Option<String>,
    pub is_deceased_indicator: Option<bool>,
    pub occupation: Option<String>,
    pub time_of_death: Option<NaiveDate>,
    pub replaced_by_pid: Option<String>,
    pub address: Vec<Option<Address>>,
}

/// Gender as enum for type-safe processing
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub enum Gender {
    Male,
    Female,
    Diverse,
    Unknown,
}

impl Gender {
    /// Converts HL7 code (M, F, O, U) to Gender
    pub fn from_hl7(code: &str) -> Option<Self> {
        match code.to_uppercase().as_str() {
            "M" => Some(Gender::Male),
            "F" => Some(Gender::Female),
            "O" => Some(Gender::Diverse),
            "U" | "UNK" => Some(Gender::Unknown),
            _ => None,
        }
    }

    /// Converts to HL7 code
    pub fn to_hl7(&self) -> &str {
        match self {
            Gender::Male => "M",
            Gender::Female => "F",
            Gender::Diverse => "O",
            Gender::Unknown => "U",
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
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub enum MaritalStatus {
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

impl MaritalStatus {
    /// Converts HL7 code to MaritalStatus based on the provided mapping logic
    pub fn from_hl7(code: &str) -> Self {
        match code.to_uppercase().as_str() {
            "A" | "E" => MaritalStatus::LegallySeparated,
            "D" => MaritalStatus::Divorced,
            "M" => MaritalStatus::Married,
            "S" => MaritalStatus::NeverMarried,
            "W" => MaritalStatus::Widowed,
            "C" => MaritalStatus::CommonLaw,
            "G" | "P" | "R" => MaritalStatus::DomesticPartner,
            "N" => MaritalStatus::Annulled,
            "I" => MaritalStatus::Interlocutory,
            "B" => MaritalStatus::Unmarried,
            _ => MaritalStatus::Unknown,
        }
    }

    /// Returns the HL7 input code(s) that map to this status
    pub fn to_hl7_codes(&self) -> Vec<&'static str> {
        match self {
            MaritalStatus::LegallySeparated => vec!["A", "E"],
            MaritalStatus::Divorced => vec!["D"],
            MaritalStatus::Married => vec!["M"],
            MaritalStatus::NeverMarried => vec!["S"],
            MaritalStatus::Widowed => vec!["W"],
            MaritalStatus::CommonLaw => vec!["C"],
            MaritalStatus::DomesticPartner => vec!["G", "P", "R"],
            MaritalStatus::Annulled => vec!["N"],
            MaritalStatus::Interlocutory => vec!["I"],
            MaritalStatus::Unmarried => vec!["B"],
            MaritalStatus::Unknown => vec!["UNK"],
        }
    }

    /// Returns the HL7 v3 MaritalStatus code
    pub fn to_v3_code(&self) -> &'static str {
        match self {
            MaritalStatus::Annulled => "A",
            MaritalStatus::CommonLaw => "C",
            MaritalStatus::Divorced => "D",
            MaritalStatus::DomesticPartner => "T",
            MaritalStatus::Interlocutory => "I",
            MaritalStatus::LegallySeparated => "L",
            MaritalStatus::Married => "M",
            MaritalStatus::NeverMarried => "S",
            MaritalStatus::Unmarried => "U",
            MaritalStatus::Widowed => "W",
            MaritalStatus::Unknown => "UNK",
        }
    }

    /// Returns the display name for the status
    pub fn display_name(&self) -> &'static str {
        match self {
            MaritalStatus::Annulled => "Annulled",
            MaritalStatus::CommonLaw => "Common Law",
            MaritalStatus::Divorced => "Divorced",
            MaritalStatus::DomesticPartner => "Domestic partner",
            MaritalStatus::Interlocutory => "Interlocutory",
            MaritalStatus::LegallySeparated => "Legally Separated",
            MaritalStatus::Married => "Married",
            MaritalStatus::NeverMarried => "Never Married",
            MaritalStatus::Unmarried => "Unmarried",
            MaritalStatus::Widowed => "Widowed",
            MaritalStatus::Unknown => "Unknown",
        }
    }

    /// Returns the HL7 v3 code system URL
    pub fn code_system_url(&self) -> &'static str {
        match self {
            MaritalStatus::Unknown => "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
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
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Address {
    pub street_and_number: Vec<Option<String>>,
    pub city: Option<String>,
    pub zip_code: Option<String>,
    pub country: Option<String>,
}

impl Address {
    pub fn new() -> Self {
        Address {
            street_and_number: vec![None],
            city: None,
            zip_code: None,
            country: None,
        }
    }
}

impl Default for Address {
    fn default() -> Self {
        Self::new()
    }
}

impl Person {
    /// Creates a new empty Person
    pub fn new() -> Self {
        Person {
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

impl Default for Person {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_person() {
        let person = Person {
            pid: Some("12345".to_string()),
            last_name: Some("Mustermann".to_string()),
            first_names: Some("Max".to_string()),
            date_of_birth: Some(NaiveDate::from_ymd_opt(1990, 1, 15).unwrap()),
            gender: Some(Gender::Male),
            ..Person::new()
        };

        assert_eq!(person.full_name(), "Max Mustermann");
        assert!(person.is_valid());
    }

    #[test]
    fn test_gender_conversion() {
        assert_eq!(Gender::from_hl7("M"), Some(Gender::Male));
        assert_eq!(Gender::from_hl7("F"), Some(Gender::Female));
        assert_eq!(Gender::from_hl7("O"), Some(Gender::Diverse));
        assert_eq!(Gender::from_hl7("X"), None);
    }

    #[test]
    fn test_marital_status_from_hl7() {
        // Test all mappings from the original match statement
        assert_eq!(
            MaritalStatus::from_hl7("A"),
            MaritalStatus::LegallySeparated
        );
        assert_eq!(
            MaritalStatus::from_hl7("E"),
            MaritalStatus::LegallySeparated
        );
        assert_eq!(MaritalStatus::from_hl7("D"), MaritalStatus::Divorced);
        assert_eq!(MaritalStatus::from_hl7("M"), MaritalStatus::Married);
        assert_eq!(MaritalStatus::from_hl7("S"), MaritalStatus::NeverMarried);
        assert_eq!(MaritalStatus::from_hl7("W"), MaritalStatus::Widowed);
        assert_eq!(MaritalStatus::from_hl7("C"), MaritalStatus::CommonLaw);
        assert_eq!(MaritalStatus::from_hl7("G"), MaritalStatus::DomesticPartner);
        assert_eq!(MaritalStatus::from_hl7("P"), MaritalStatus::DomesticPartner);
        assert_eq!(MaritalStatus::from_hl7("R"), MaritalStatus::DomesticPartner);
        assert_eq!(MaritalStatus::from_hl7("N"), MaritalStatus::Annulled);
        assert_eq!(MaritalStatus::from_hl7("I"), MaritalStatus::Interlocutory);
        assert_eq!(MaritalStatus::from_hl7("B"), MaritalStatus::Unmarried);
        assert_eq!(MaritalStatus::from_hl7("X"), MaritalStatus::Unknown);
        assert_eq!(MaritalStatus::from_hl7(""), MaritalStatus::Unknown);
    }

    #[test]
    fn test_marital_status_v3_codes() {
        assert_eq!(MaritalStatus::LegallySeparated.to_v3_code(), "L");
        assert_eq!(MaritalStatus::Divorced.to_v3_code(), "D");
        assert_eq!(MaritalStatus::Married.to_v3_code(), "M");
        assert_eq!(MaritalStatus::NeverMarried.to_v3_code(), "S");
        assert_eq!(MaritalStatus::Widowed.to_v3_code(), "W");
        assert_eq!(MaritalStatus::CommonLaw.to_v3_code(), "C");
        assert_eq!(MaritalStatus::DomesticPartner.to_v3_code(), "T");
        assert_eq!(MaritalStatus::Annulled.to_v3_code(), "A");
        assert_eq!(MaritalStatus::Interlocutory.to_v3_code(), "I");
        assert_eq!(MaritalStatus::Unmarried.to_v3_code(), "U");
        assert_eq!(MaritalStatus::Unknown.to_v3_code(), "UNK");
    }

    #[test]
    fn test_marital_status_display_names() {
        assert_eq!(
            MaritalStatus::LegallySeparated.display_name(),
            "Legally Separated"
        );
        assert_eq!(
            MaritalStatus::DomesticPartner.display_name(),
            "Domestic partner"
        );
        assert_eq!(MaritalStatus::NeverMarried.display_name(), "Never Married");
    }

    #[test]
    fn test_marital_status_coding() {
        let (system, code, display) = MaritalStatus::Married.to_coding();
        assert_eq!(
            system,
            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"
        );
        assert_eq!(code, "M");
        assert_eq!(display, "Married");

        let (system, code, display) = MaritalStatus::Unknown.to_coding();
        assert_eq!(
            system,
            "http://terminology.hl7.org/CodeSystem/v3-NullFlavor"
        );
        assert_eq!(code, "UNK");
        assert_eq!(display, "Unknown");
    }

    #[test]
    fn test_address() {
        let address = Address {
            street_and_number: vec![Some("Main Street 42".to_string())],
            city: Some("Sample City".to_string()),
            zip_code: Some("12345".to_string()),
            country: Some("Germany".to_string()),
        };

        let person = Person {
            address: vec![Some(address)],
            ..Person::new()
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
        let person = Person {
            title: Some("Dr.".to_string()),
            first_names: Some("Max".to_string()),
            last_name: Some("Mustermann".to_string()),
            name_suffix: Some("Jr.".to_string()),
            ..Person::new()
        };

        assert_eq!(person.full_name(), "Dr. Max Mustermann Jr.");
    }

    #[test]
    fn test_invalid_person() {
        let person = Person::new();
        assert!(!person.is_valid());
    }
}
