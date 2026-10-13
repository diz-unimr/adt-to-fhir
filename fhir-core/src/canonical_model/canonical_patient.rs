use crate::canonical_model::canonical_meta::MappingOpPatient;
use chrono::NaiveDate;
use derive_builder::Builder;
use fhir_model::time::OffsetDateTime;

/// Main structure for person data

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct PatientCdm {
    pub meta: MappingOpPatient,
    pub pid: String,

    /// note: use only for observation mapping - never for encounter
    #[builder(default)]
    pub encounter_number: Option<String>,
    #[builder(default)]
    pub gender: Option<GenderCdm>,
    #[builder(default)]
    pub date_of_birth: Option<CdmDates>,
    #[builder(default)]
    pub marital_status: Option<MaritalStatusCdm>,
    #[builder(default)]
    pub nationality: Option<String>,
    #[builder(default)]
    pub is_multiple_birth: Option<bool>,
    #[builder(default)]
    pub names: Vec<Option<PersonName>>,
    #[builder(default)]
    pub multiple_birth_order: Option<u32>,
    #[builder(default)]
    pub mother_case_number_birth: Option<String>,
    #[builder(default)]
    pub is_deceased_indicator: Option<bool>,
    #[builder(default)]
    pub occupation: Option<String>,
    #[builder(default)]
    pub time_of_death: Option<CdmDates>,
    #[builder(default)]
    pub replaced_by_pid: Option<String>,
    #[builder(default)]
    pub address: Vec<Option<AddressCdm>>,
    #[builder(default)]
    pub insurance: Vec<Option<Insurance>>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum CdmDates {
    Date(NaiveDate),
    Datetime(OffsetDateTime),
}
impl PatientCdm {
    pub fn id(&self) -> &String {
        &self.meta.id
    }
}
#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct Insurance {
    #[builder(default)]
    pub insurance_number: String,
    #[builder(default)]
    pub assigner_id: String,
    #[builder(default)]
    pub valid_from: Option<NaiveDate>,
    #[builder(default)]
    pub valid_to: Option<NaiveDate>,
    #[builder(default)]
    pub insurance_type: Option<InsuranceType>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum InsuranceType {
    GkvPkv,
    Other,
}

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct PersonName {
    /// Family name (often called 'Surname')
    #[builder(default)]
    pub family: Option<String>,

    /// official (false) or maiden (true)
    #[builder(default)]
    pub is_maiden: Option<bool>,

    /// Given names (not always 'first'). Includes middle names
    #[builder(default)]
    pub given_name: Vec<Option<String>>,
    /// Parts that come before the name
    #[builder(default)]
    pub name_prefix: Option<String>,
    /// namenszusatz
    #[builder(default)]
    pub name_extension: Option<String>,
    /// Parts that come after the name (vorsatzwort)
    #[builder(default)]
    pub name_affix: Option<String>,
}

/// Gender as enum for type-safe processing
#[derive(Debug, Clone, PartialEq)]
pub enum GenderCdm {
    Male,
    Female,
    Diverse,
    Unknown,
}

impl GenderCdm {
    /// Converts HL7 code (M, F, O, U) to Gender
    pub fn from_hl7(code: &str) -> Option<Self> {
        match code.to_uppercase().as_str() {
            "M" => Some(GenderCdm::Male),
            "F" => Some(GenderCdm::Female),
            "O" => Some(GenderCdm::Diverse),
            "U" | "UNK" => Some(GenderCdm::Unknown),
            _ => None,
        }
    }

    /// Converts to HL7 code
    pub fn to_hl7(&self) -> &str {
        match self {
            GenderCdm::Male => "M",
            GenderCdm::Female => "F",
            GenderCdm::Diverse => "O",
            GenderCdm::Unknown => "U",
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
pub enum MaritalStatusCdm {
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

impl MaritalStatusCdm {
    /// Converts HL7 code to MaritalStatus based on the provided mapping logic
    pub fn from_hl7(code: &str) -> Self {
        match code.to_uppercase().as_str() {
            "A" | "E" => MaritalStatusCdm::LegallySeparated,
            "D" => MaritalStatusCdm::Divorced,
            "M" => MaritalStatusCdm::Married,
            "S" => MaritalStatusCdm::NeverMarried,
            "W" => MaritalStatusCdm::Widowed,
            "C" => MaritalStatusCdm::CommonLaw,
            "G" | "P" | "R" => MaritalStatusCdm::DomesticPartner,
            "N" => MaritalStatusCdm::Annulled,
            "I" => MaritalStatusCdm::Interlocutory,
            "B" => MaritalStatusCdm::Unmarried,
            _ => MaritalStatusCdm::Unknown,
        }
    }

    /// Returns the HL7 v3 MaritalStatus code
    pub fn to_v3_code(&self) -> &'static str {
        match self {
            MaritalStatusCdm::Annulled => "A",
            MaritalStatusCdm::CommonLaw => "C",
            MaritalStatusCdm::Divorced => "D",
            MaritalStatusCdm::DomesticPartner => "T",
            MaritalStatusCdm::Interlocutory => "I",
            MaritalStatusCdm::LegallySeparated => "L",
            MaritalStatusCdm::Married => "M",
            MaritalStatusCdm::NeverMarried => "S",
            MaritalStatusCdm::Unmarried => "U",
            MaritalStatusCdm::Widowed => "W",
            MaritalStatusCdm::Unknown => "UNK",
        }
    }

    /// Returns the display name for the status
    pub fn display_name(&self) -> &'static str {
        match self {
            MaritalStatusCdm::Annulled => "Annulled",
            MaritalStatusCdm::CommonLaw => "Common Law",
            MaritalStatusCdm::Divorced => "Divorced",
            MaritalStatusCdm::DomesticPartner => "Domestic partner",
            MaritalStatusCdm::Interlocutory => "Interlocutory",
            MaritalStatusCdm::LegallySeparated => "Legally Separated",
            MaritalStatusCdm::Married => "Married",
            MaritalStatusCdm::NeverMarried => "Never Married",
            MaritalStatusCdm::Unmarried => "Unmarried",
            MaritalStatusCdm::Widowed => "Widowed",
            MaritalStatusCdm::Unknown => "Unknown",
        }
    }

    /// Returns the HL7 v3 code system URL
    pub fn code_system_url(&self) -> &'static str {
        match self {
            MaritalStatusCdm::Unknown => "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
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
#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct AddressCdm {
    pub street_and_number: Vec<Option<String>>,
    pub city: Option<String>,
    pub zip_code: Option<String>,
    pub country: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_model::canonical_meta::ProcessingOperation;

    #[test]
    fn test_gender_conversion() {
        assert_eq!(GenderCdm::from_hl7("M"), Some(GenderCdm::Male));
        assert_eq!(GenderCdm::from_hl7("F"), Some(GenderCdm::Female));
        assert_eq!(GenderCdm::from_hl7("O"), Some(GenderCdm::Diverse));
        assert_eq!(GenderCdm::from_hl7("X"), None);
    }

    #[test]
    fn test_marital_status_from_hl7() {
        // Test all mappings from the original match statement
        assert_eq!(
            MaritalStatusCdm::from_hl7("A"),
            MaritalStatusCdm::LegallySeparated
        );
        assert_eq!(
            MaritalStatusCdm::from_hl7("E"),
            MaritalStatusCdm::LegallySeparated
        );
        assert_eq!(MaritalStatusCdm::from_hl7("D"), MaritalStatusCdm::Divorced);
        assert_eq!(MaritalStatusCdm::from_hl7("M"), MaritalStatusCdm::Married);
        assert_eq!(
            MaritalStatusCdm::from_hl7("S"),
            MaritalStatusCdm::NeverMarried
        );
        assert_eq!(MaritalStatusCdm::from_hl7("W"), MaritalStatusCdm::Widowed);
        assert_eq!(MaritalStatusCdm::from_hl7("C"), MaritalStatusCdm::CommonLaw);
        assert_eq!(
            MaritalStatusCdm::from_hl7("G"),
            MaritalStatusCdm::DomesticPartner
        );
        assert_eq!(
            MaritalStatusCdm::from_hl7("P"),
            MaritalStatusCdm::DomesticPartner
        );
        assert_eq!(
            MaritalStatusCdm::from_hl7("R"),
            MaritalStatusCdm::DomesticPartner
        );
        assert_eq!(MaritalStatusCdm::from_hl7("N"), MaritalStatusCdm::Annulled);
        assert_eq!(
            MaritalStatusCdm::from_hl7("I"),
            MaritalStatusCdm::Interlocutory
        );
        assert_eq!(MaritalStatusCdm::from_hl7("B"), MaritalStatusCdm::Unmarried);
        assert_eq!(MaritalStatusCdm::from_hl7("X"), MaritalStatusCdm::Unknown);
        assert_eq!(MaritalStatusCdm::from_hl7(""), MaritalStatusCdm::Unknown);
    }

    #[test]
    fn test_marital_status_v3_codes() {
        assert_eq!(MaritalStatusCdm::LegallySeparated.to_v3_code(), "L");
        assert_eq!(MaritalStatusCdm::Divorced.to_v3_code(), "D");
        assert_eq!(MaritalStatusCdm::Married.to_v3_code(), "M");
        assert_eq!(MaritalStatusCdm::NeverMarried.to_v3_code(), "S");
        assert_eq!(MaritalStatusCdm::Widowed.to_v3_code(), "W");
        assert_eq!(MaritalStatusCdm::CommonLaw.to_v3_code(), "C");
        assert_eq!(MaritalStatusCdm::DomesticPartner.to_v3_code(), "T");
        assert_eq!(MaritalStatusCdm::Annulled.to_v3_code(), "A");
        assert_eq!(MaritalStatusCdm::Interlocutory.to_v3_code(), "I");
        assert_eq!(MaritalStatusCdm::Unmarried.to_v3_code(), "U");
        assert_eq!(MaritalStatusCdm::Unknown.to_v3_code(), "UNK");
    }

    #[test]
    fn test_marital_status_display_names() {
        assert_eq!(
            MaritalStatusCdm::LegallySeparated.display_name(),
            "Legally Separated"
        );
        assert_eq!(
            MaritalStatusCdm::DomesticPartner.display_name(),
            "Domestic partner"
        );
        assert_eq!(
            MaritalStatusCdm::NeverMarried.display_name(),
            "Never Married"
        );
    }

    #[test]
    fn test_marital_status_coding() {
        let (system, code, display) = MaritalStatusCdm::Married.to_coding();
        assert_eq!(
            system,
            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"
        );
        assert_eq!(code, "M");
        assert_eq!(display, "Married");

        let (system, code, display) = MaritalStatusCdm::Unknown.to_coding();
        assert_eq!(
            system,
            "http://terminology.hl7.org/CodeSystem/v3-NullFlavor"
        );
        assert_eq!(code, "UNK");
        assert_eq!(display, "Unknown");
    }

    #[test]
    fn test_address() {
        let address = AddressCdm {
            street_and_number: vec![Some("Main Street 42".to_string())],
            city: Some("Sample City".to_string()),
            zip_code: Some("12345".to_string()),
            country: Some("Germany".to_string()),
        };
        let mut person = get_minimal_person();
        person.address = vec![Some(address)];

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

    fn get_minimal_person() -> PatientCdm {
        PatientCdmBuilder::default()
            .meta(MappingOpPatient {
                id: "42".to_string(),
                operation: ProcessingOperation::UpdateAsCreate,
            })
            .pid("42")
            .gender(GenderCdm::Unknown)
            .build()
            .unwrap()
    }
}
