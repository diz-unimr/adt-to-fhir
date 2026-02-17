use crate::config::Fhir;
use crate::fhir::mapper::MessageAccessError::{MissingMessageField, MissingMessageSegment};
use crate::fhir::mapper::MessageType::*;
use crate::fhir::mapper::MessageTypeError::MissingMessageType;
use crate::fhir::resources::ResourceMap;
use crate::fhir::{encounter, patient};
use anyhow::anyhow;
use chrono::{Datelike, NaiveDate, NaiveDateTime, ParseError, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::codes::HTTPVerb::Patch;
use fhir_model::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, IdentifiableResource, Parameters, Resource,
    ResourceType,
};
use fhir_model::r4b::types::{Identifier, Reference};
use fhir_model::time::error::InvalidFormatDescription;
use fhir_model::time::{Month, OffsetDateTime};
use fhir_model::{BuilderError, DateFormatError, Instant};
use fhir_model::{Date, DateTime, time};
use fmt::Display;
use hl7_parser::Message;
use hl7_parser::message::Segment;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum MappingError {
    #[error(transparent)]
    MessageAccessError(#[from] MessageAccessError),
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    #[error(transparent)]
    FormattingError(#[from] FormattingError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum FormattingError {
    #[error(transparent)]
    DateFormatError(#[from] DateFormatError),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error(transparent)]
    ParseDateError(#[from] time::error::Parse),
    #[error(transparent)]
    InvalidFormatError(#[from] InvalidFormatDescription),
    #[error(transparent)]
    ComponentRangeError(#[from] time::error::ComponentRange),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum MessageAccessError {
    #[error("Missing message segment {0}")]
    MissingMessageSegment(String),
    #[error("Missing message field {0} in segment {1}")]
    MissingMessageField(String, String),
    #[error(transparent)]
    MessageTypeError(#[from] MessageTypeError),
    #[error(transparent)]
    ParseError(#[from] hl7_parser::parser::ParseError),
}

#[derive(Clone)]
pub(crate) struct FhirMapper {
    pub(crate) config: Fhir,
    pub(crate) resources: ResourceMap,
}

impl FhirMapper {
    pub(crate) fn new(config: Fhir) -> Result<Self, anyhow::Error> {
        Ok(FhirMapper {
            config,
            resources: ResourceMap::new()?,
        })
    }

    pub(crate) fn map(&self, msg: String) -> Result<Option<String>, anyhow::Error> {
        // deserialize
        let v2_msg = Message::parse_with_lenient_newlines(msg.as_str(), true)?;

        // map hl7 message
        let resources = self.map_resources(&v2_msg)?;

        if resources.is_empty() {
            return Ok(None);
        }

        let result = Bundle::builder()
            .r#type(BundleType::Transaction)
            .entry(resources)
            .build()?;

        // serialize
        let result = serde_json::to_string(&result).expect("failed to serialize output bundle");

        Ok(Some(result))
    }

    fn map_resources(&self, v2_msg: &Message) -> Result<Vec<Option<BundleEntry>>, MappingError> {
        let p = patient::map(v2_msg, self.config.clone())?;
        let e = encounter::map(v2_msg, self.config.clone(), &self.resources)?;
        // TODO map observation
        let res = p.into_iter().chain(e).map(Some).collect();

        Ok(res)
    }
}

#[derive(PartialEq, Debug)]
pub enum MessageType {
    /// ADT A01
    Admit,
    /// ADT A02
    Transfer,
    /// ADT A03
    Discharge,
    /// ADT A04
    Registration,
    /// ADT A05
    PreAdmit,
    /// ADT A06
    ChangeOutpatientToInpatient,
    /// ADT A07
    ChangeInpatientToOutpatient,
    /// ADT A08
    PatientUpdate,
    /// ADT A11
    CancelAdmitVisit,
    /// ADT A12
    CancelTransfer,
    /// ADT A13
    CancelDischarge,
    /// ADT A14
    PendingAdmit,
    /// ADT A27
    CancelPendingAdmit,
    /// ADT A28
    AddPersonInformation,
    /// ADT A29
    DeletePersonInformation,
    /// ADT A31
    ChangePersonData,
    /// ADT A34
    PatientMerge,
    /// ADT A40
    MergePatientRecords,
    /// ADT A45
    PatientReassignmentToSingleCase,
    /// ADT A47
    PatientReassignmentToAllCases,
    /// ADT A50
    UpdateEncounterNumber,
}

impl Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Admit => write!(f, "A01"),
            Transfer => write!(f, "A02"),
            Discharge => write!(f, "A03"),
            Registration => write!(f, "A04"),
            PreAdmit => write!(f, "A05"),
            ChangeOutpatientToInpatient => write!(f, "A06"),
            ChangeInpatientToOutpatient => write!(f, "A07"),
            PatientUpdate => write!(f, "A08"),
            CancelAdmitVisit => write!(f, "A11"),
            CancelTransfer => write!(f, "A12"),
            CancelDischarge => write!(f, "A13"),
            PendingAdmit => write!(f, "A14"),
            CancelPendingAdmit => write!(f, "A27"),
            AddPersonInformation => write!(f, "A28"),
            DeletePersonInformation => write!(f, "A29"),
            ChangePersonData => write!(f, "A31"),
            PatientMerge => write!(f, "A34"),
            MergePatientRecords => write!(f, "A40"),
            PatientReassignmentToSingleCase => write!(f, "A45"),
            PatientReassignmentToAllCases => write!(f, "A47"),
            UpdateEncounterNumber => write!(f, "A50"),
        }
    }
}

#[derive(Debug, Error)]
pub enum MessageTypeError {
    #[error("Unknown message type: {0}")]
    UnknownMessageType(String),
    #[error("Missing message type: {0}")]
    MissingMessageType(String),
}

// todo refactor
impl FromStr for MessageType {
    type Err = MessageTypeError;

    fn from_str(s: &str) -> Result<Self, MessageTypeError> {
        match s {
            "A01" => Ok(Admit),
            "A02" => Ok(Transfer),
            "A03" => Ok(Discharge),
            "A04" => Ok(Registration),
            "A05" => Ok(PreAdmit),
            "A06" => Ok(ChangeOutpatientToInpatient),
            "A07" => Ok(ChangeInpatientToOutpatient),
            "A08" => Ok(PatientUpdate),
            "A11" => Ok(CancelAdmitVisit),
            "A12" => Ok(CancelTransfer),
            "A13" => Ok(CancelDischarge),
            "A14" => Ok(PendingAdmit),
            "A27" => Ok(CancelPendingAdmit),
            "A28" => Ok(AddPersonInformation),
            "A29" => Ok(DeletePersonInformation),
            "A31" => Ok(ChangePersonData),
            "A34" => Ok(PatientMerge),
            "A40" => Ok(MergePatientRecords),
            "A45" => Ok(PatientReassignmentToSingleCase),
            "A47" => Ok(PatientReassignmentToAllCases),
            "A50" => Ok(UpdateEncounterNumber),
            _ => Err(MessageTypeError::UnknownMessageType(s.to_string())),
        }
    }
}

pub(crate) fn message_type(msg: &Message) -> Result<MessageType, MessageTypeError> {
    MessageType::from_str(
        msg.segment("EVN")
            .ok_or(MissingMessageType("missing EVN segment".to_string()))?
            .field(1)
            .ok_or(MissingMessageType(
                "missing message type segment".to_string(),
            ))?
            .raw_value(),
    )
}

pub(crate) enum EntryRequestType {
    UpdateAsCreate,
    ConditionalCreate,
    Delete,
}

pub(crate) fn bundle_entry<T: IdentifiableResource + Clone>(
    resource: T,
    request_type: EntryRequestType,
) -> Result<BundleEntry, MappingError>
where
    Resource: From<T>,
{
    // resource
    let r = Resource::from(resource.clone());

    // identifier
    let identifier = resource
        .identifier()
        .iter()
        .flatten()
        .find(|&id| id.r#use.is_some_and(|u| u == IdentifierUse::Usual))
        .ok_or(anyhow!("missing identifier with use: 'usual'"))?;

    // resource type
    let resource_type = r.resource_type();

    let request = bundle_entry_request(resource_type, identifier, request_type)?;

    BundleEntry::builder()
        .resource(r)
        .request(request)
        .build()
        .map_err(|e| e.into())
}

fn bundle_entry_request(
    resource_type: ResourceType,
    identifier: &Identifier,
    request_type: EntryRequestType,
) -> Result<BundleEntryRequest, MappingError> {
    Ok(match request_type {
        EntryRequestType::UpdateAsCreate => BundleEntryRequest::builder()
            .method(HTTPVerb::Put)
            .url(conditional_reference(&resource_type, identifier)?)
            .build()?,

        EntryRequestType::ConditionalCreate => BundleEntryRequest::builder()
            .method(HTTPVerb::Post)
            .url(resource_type.to_string())
            .if_none_exist(conditional_reference(&resource_type, identifier)?)
            .build()?,

        EntryRequestType::Delete => BundleEntryRequest::builder()
            .method(HTTPVerb::Delete)
            .url(conditional_reference(&resource_type, identifier)?)
            .build()?,
    })
}

pub(crate) fn patch_bundle_entry(
    resource: Parameters,
    resource_type: &ResourceType,
    identifier: &Identifier,
) -> Result<BundleEntry, MappingError> {
    let request = BundleEntryRequest::builder()
        .method(Patch)
        .url(conditional_reference(resource_type, identifier)?)
        .build()?;

    BundleEntry::builder()
        .resource(resource.into())
        .request(request)
        .build()
        .map_err(|e| e.into())
}

pub(crate) fn conditional_reference(
    resource_type: &ResourceType,
    identifier: &Identifier,
) -> Result<String, MappingError> {
    Ok(format!(
        "{resource_type}?{}",
        identifier_search(
            identifier
                .system
                .as_deref()
                .ok_or(anyhow!("identifier.system missing"))?,
            identifier
                .value
                .as_deref()
                .ok_or(anyhow!("identifier.value missing"))?
        )
    ))
}

fn identifier_search(system: &str, value: &str) -> String {
    format!("identifier={system}|{value}")
}

fn default_identifier(identifiers: Vec<Option<Identifier>>) -> Option<Identifier> {
    if identifiers.iter().flatten().count() == 1 {
        identifiers.into_iter().next()?
    } else {
        identifiers
            .iter()
            .flatten()
            .filter_map(|i| {
                // use USUAL identifier for now
                if i.r#use? == IdentifierUse::Usual {
                    Some(i.clone())
                } else {
                    None
                }
            })
            .next()
    }
}

pub(crate) fn parse_component(
    field: &str,
    component: usize,
) -> Result<Option<String>, hl7_parser::parser::ParseError> {
    let comp = hl7_parser::parser::parse_field(field)?
        .component(component)
        .map(|c| c.raw_value().to_string())
        .filter(|s| !s.is_empty());

    Ok(comp)
}

pub(crate) fn parse_subcomponents(
    field: &str,
    component: usize,
) -> Result<Option<Vec<String>>, hl7_parser::parser::ParseError> {
    let comp: Option<Vec<String>> = hl7_parser::parser::parse_field(field)?
        .component(component)
        .map(|c| {
            c.subcomponents
                .iter()
                .map(|s| s.raw_value().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });

    Ok(comp)
}

pub(crate) fn parse_date(input: &str) -> Result<Date, FormattingError> {
    let dt = NaiveDate::parse_and_remainder(input, "%Y%m%d")?.0;
    let date = time::Date::from_calendar_date(
        dt.year(),
        Month::try_from(dt.month() as u8)?,
        dt.day() as u8,
    )?;
    Ok(Date::Date(date))
}

pub(crate) fn parse_datetime(input: &str) -> Result<DateTime, FormattingError> {
    let dt = NaiveDateTime::parse_from_str(input, "%Y%m%d%H%M")?;
    let dt_with_tz = Berlin
        .from_local_datetime(&dt)
        .earliest()
        .ok_or(InvalidDate)?;

    Ok(DateTime::DateTime(Instant(
        OffsetDateTime::from_unix_timestamp(dt_with_tz.timestamp())?,
    )))
}
pub(crate) fn resource_ref(
    res_type: &ResourceType,
    id: &str,
    system: &str,
) -> Result<Reference, MappingError> {
    Ok(Reference::builder()
        .reference(format!(
            "{res_type}?{}",
            identifier_search(system, id)
        ))
        .build()?)
}

pub(crate) fn parse_field(
    msg: &Message,
    segment: &str,
    field: usize,
) -> Result<Option<String>, MessageAccessError> {
    Ok(Some(
        msg.segment(segment)
            .ok_or(MissingMessageSegment(segment.to_string()))?
            .field(field)
            .ok_or(MissingMessageField(field.to_string(), segment.to_string()))?
            .raw_value()
            .to_string(),
    )
    .filter(|f| !f.is_empty()))
}

pub(crate) fn parse_segments_field(
    msg: &Message,
    segment: &str,
    field: usize,
) -> Result<Vec<Option<String>>, MessageAccessError> {
    let vec_size_needed = msg.segment_count(segment);
    if vec_size_needed < 1 {
        return Err(MissingMessageSegment(segment.to_string()));
    }
    let mut result: Vec<Option<String>> = Vec::with_capacity(vec_size_needed);
    for idx in 1..vec_size_needed + 1 {
        match msg.segment_n(segment, idx) {
            Some(segment_of_index) => match segment_of_index.field(field) {
                Some(raw) => result.push(Some(raw.raw_value().to_string())),
                None => return Err(MissingMessageField(field.to_string(), segment.to_string())),
            },
            None => result.push(None),
        }
    }
    Ok(result)
}

/// Extraktion eines Werts aus einem Segment
/// # Arguments
/// * `segment` - Referenz des Segments aus dem wir Informationen lesen wollen
/// * `field_number` - 1 basierter Feld-Index des Ziels
/// * `repeat_index` - 0 basierter Index der Feldwiederholungen
/// * `component_number` - 1 basierter Komponenten-Index des ausgewählten Repeats
/// # Result
/// `String`-Wert des Eintrags. Sollte einer oder mehre Indexe außerhalb der verfügbaren Felder
/// liegen, so wird `None` zurückgeliefert.
///
pub(crate) fn get_repeat_value(
    segment: &Segment,
    field_number: usize,
    repeat_index: usize,
    component_number: usize,
) -> Option<String> {
    if field_number == 0 || component_number == 0 {
        return None;
    }

    match segment
        .fields()
        .nth(field_number - 1)?
        .repeats()
        .nth(repeat_index)?
        .component(component_number)
    {
        Some(value) => {
            if value.is_empty() {
                None
            } else {
                Some(value.raw_value().to_string())
            }
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{FallConfig, Fhir, PatientConfig, ResourceConfig};
    use crate::fhir::mapper::{
        FhirMapper, FormattingError, MessageAccessError, get_repeat_value, parse_component,
        parse_datetime, parse_segments_field, parse_subcomponents, patch_bundle_entry,
    };
    use crate::fhir::mapper::{Identifier, parse_field};
    use crate::fhir::resources::{Department, ResourceMap};
    use crate::tests::read_test_resource;
    use fhir_model::DateTime::{Date, DateTime};
    use fhir_model::r4b::codes::HTTPVerb::Patch;
    use fhir_model::r4b::resources::{
        Bundle, BundleEntry, BundleEntryRequest, Encounter, Parameters, Patient, Resource,
        ResourceType,
    };
    use fhir_model::time::{Month, OffsetDateTime, Time};
    use fhir_model::{WrongResourceType, time};
    use hl7_parser::Message;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::fmt::Debug;

    #[test]
    fn test_parse_datetime() {
        // 2009-03-30 19:36
        let s = "200903301036";

        let parsed = parse_datetime(s).unwrap();

        let expected = DateTime(
            OffsetDateTime::new_utc(
                time::Date::from_calendar_date(2009, Month::March, 30).unwrap(),
                // local time is +2 (CEST) in this case
                Time::from_hms(8, 36, 0).unwrap(),
            )
            .into(),
        );

        assert_eq!(parsed, expected);
    }

    #[test]
    fn map_test() {
        let hl7 = read_test_resource("a01_test.hl7");

        let config = Fhir {
            facility_id: "260620431".to_string(),
            person: PatientConfig {
                profile: "https://www.medizininformatik-initiative.de/fhir/core/modul-person/StructureDefinition/Patient|2025.0.0".to_string(),
                system: "https://fhir.diz.uni-marburg.de/sid/patient-id".to_string(),
                other_insurance_system: "https://fhir.diz.uni-marburg.de/sid/patient-other-insurance-id".to_string()
            },
            fall: FallConfig {
                profile: "https://www.medizininformatik-initiative.de/fhir/core/modul-fall/StructureDefinition/KontaktGesundheitseinrichtung|2025.0.0".to_string(),
                system: "https://fhir.diz.uni-marburg.de/sid/encounter-id".to_string(),
                einrichtungskontakt: Default::default(),
                abteilungskontakt: Default::default(),
                versorgungsstellenkontakt: Default::default(),
            },
        };
        let mapper = FhirMapper {
            config: config.clone(),
            resources: ResourceMap {
                department_map: HashMap::from([(
                    "POL".to_string(),
                    Department {
                        abteilungs_bezeichnung: "Pneumologie".to_string(),
                        fachabteilungs_schluessel: "0800".to_string(),
                    },
                )]),
                location_map: Default::default(),
            },
        };

        // act
        let mapped = mapper.map(hl7).unwrap();

        // map back to assert
        let bundle: Bundle = serde_json::from_str(mapped.unwrap().as_str()).unwrap();

        assert_eq!(bundle.entry.len(), 2);

        let patient: Vec<Patient> = bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| to_patient(e).ok())
            .collect();
        let encounter: Vec<Encounter> = bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| to_encounter(e).ok())
            .collect();

        // assert profiles set
        let p = patient.first().unwrap();
        let patient_profile = p
            .meta
            .as_ref()
            .unwrap()
            .profile
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_str();
        let e = encounter.first().unwrap();
        let encounter_profile = e
            .meta
            .as_ref()
            .unwrap()
            .profile
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_str();

        assert_eq!(patient_profile, config.person.profile.to_owned());
        assert_eq!(encounter_profile, config.fall.profile.to_owned());
    }

    fn to_patient(e: &BundleEntry) -> Result<Patient, WrongResourceType> {
        let r = e.resource.clone().unwrap();
        Patient::try_from(r)
    }

    fn to_encounter(e: &BundleEntry) -> Result<Encounter, WrongResourceType> {
        let r = e.resource.clone().unwrap();
        Encounter::try_from(r)
    }

    #[test]
    fn test_parse_component() {
        let comp = parse_component("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L", 3).unwrap();

        assert_eq!(comp, Some("Holzhausen".to_string()))
    }

    #[test]
    fn test_parse_subcomponent() {
        let sub =
            parse_subcomponents("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L", 1).unwrap();

        assert_eq!(
            sub,
            Some(vec![
                "Talstraße 16".to_string(),
                "Talstraße".to_string(),
                "16".to_string()
            ])
        )
    }

    #[test]
    fn test_patch_bundle_entry() {
        let entry = patch_bundle_entry(
            Parameters::builder().build().unwrap(),
            &ResourceType::Patient,
            &Identifier::builder()
                .system("system".to_string())
                .value("value".to_string())
                .build()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(
            entry,
            BundleEntry::builder()
                .resource(Resource::from(Parameters::builder().build().unwrap()))
                .request(
                    BundleEntryRequest::builder()
                        .method(Patch)
                        .url("Patient?identifier=system|value".to_string())
                        .build()
                        .unwrap()
                )
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_read_multiple_segment_same_name() {
        let test_msg=
        Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE||||N
PV1|1|I|KJMST042^BSP-2-2^^KJM^KLINIKUM^000000|R^^HL7~01^Normalfall^11||KJMST042^BSP-1-1^^KJM^KLINIKUM^000000||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01|||1000|9||||202511022120|202511022120||||||A
PV2|||06^Geburt^11||||||202511022120|||Versicherten Nr. der Mutter 0000000000||||||||||N||I||||||||||||Y
DG1|1||Z38.0^Einling, Geburt im Krankenhaus^icd10gm2023||0000000000000|FA En|||||||||1|BBBBBB^^^^^^^^^^^^^^^^^^^^^^GEB||||12340005|U
DG1|2||Z38.0^Einling, Geburt im Krankenhaus^icd10gm2023||0000000000000|FA Be|||||||||2|BBBBBB^^^^^^^^^^^^^^^^^^^^^^KJM||||12340007|U
DG1|3||Z38.0^Einling, Geburt im Krankenhaus^icd10gm2023||0000000000000|Aufn.|||||||||1|BBBBBB^^^^^^^^^^^^^^^^^^^^^^GEB||||12340009|U
DG1|4||Z38.0^Einling, Geburt im Krankenhaus^icd10gm2023||0000000000000|FA Be|||||||||1|BBBBBB^^^^^^^^^^^^^^^^^^^^^^GEB||||12340001|U
IN1|1||000000000^^^^NII~Krankenkasse^^^^XX|Krankenkasse|Strasse 1&Strasse&1^^Stadt^^1000^DE^L||000000000000^PRN^PH^^^00000^0000000^^^^^000000000000||Krankenkasse^1^^^1&gesetzliche Krankenkasse^^NII~Krankenkasse^1^^^^^U|||||||Nachname^Vorname||19340101|Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L|||H|||||||||F||||||||||||F|||||||||AndereStadt
IN2|1||||||||||||||||||||||||||||^PC^100.0||||DE|||N|||ev||||||||||||||||||||||||00000 0000000
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
ZNG|1|N|N|Normal|L|48|3390|||Gesundes Neugeborenes"#,true).unwrap();

        let actual = parse_segments_field(&test_msg, "DG1", 1);

        assert_eq!(4, actual.as_ref().unwrap().len());

        for idx in 0..3 {
            let value = actual.as_ref().unwrap();
            assert_eq!((idx + 1).to_string(), value[idx].clone().unwrap());
        }
    }

    #[rstest]
    #[case(3, 0, 1, Some("777777777"))]
    #[case(3, 0, 5, Some("NII"))]
    #[case(3, 1, 5, Some("XX"))]
    #[case(300, 0, 5, None)]
    #[case(3, 100, 5, None)]
    #[case(3, 1, 500, None)]
    #[case(0, 0, 0, None)]
    fn test_get_repeat_value(
        #[case] field_number: usize,
        #[case] repeat_index: usize,
        #[case] component_number: usize,
        #[case] expected: Option<&str>,
    ) {
        let segment_raw: &str = r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
IN1|2||777777777^^^^NII~BG HM HAUPT^^^^XX|BGHM - Hauptverwaltung|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||000000000001^PRN^PH^^^0800^99900801^^^^^000000000001~1313131331313^PRN^FX^^^00000^00000000^^^^^1313131331313||Träger der ges. Unfallversicherer^26^^^2&Berufsgenossenschaft^^NII~Träger der ges. Unfallversicherer^26^^^^^U||||||10001|Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||H|||||||||M||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L"#;
        let msg = Message::parse_with_lenient_newlines(segment_raw, true).unwrap();

        let segment = msg.segment("IN1").unwrap();
        let result = get_repeat_value(segment, field_number, repeat_index, component_number);

        match expected {
            None => {
                assert_eq!(None, result);
            }
            Some(expect_value) => {
                assert_eq!(expect_value, result.unwrap());
            }
        }
    }
}
