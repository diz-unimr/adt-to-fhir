use crate::config::Fhir;
use crate::fhir::mapper::MessageAccessError::MissingMessageSegment;
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
use hl7_parser::message::{Field, Repeat, Segment};
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

pub(crate) fn parse_component(field: &Field, component: usize) -> Option<String> {
    parse_repeat_component(field.repeats.first()?, component)
}

pub(crate) fn parse_repeat_component(repeat: &Repeat, component: usize) -> Option<String> {
    repeat
        .component(component)
        .map(|c| c.raw_value().to_string())
        .filter(|s| !s.is_empty())
}

pub(crate) fn parse_subcomponents(repeat: &Repeat, component: usize) -> Option<Vec<String>> {
    repeat.component(component).map(|c| {
        c.subcomponents
            .iter()
            .map(|s| s.raw_value().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    })
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
        .reference(format!("{res_type}?{}", identifier_search(system, id)))
        .build()?)
}

pub(crate) fn parse_field<'a>(
    msg: &'a Message<'a>,
    segment: &str,
    field: usize,
) -> Result<Option<&'a Field<'a>>, MessageAccessError> {
    Ok(msg
        .segment(segment)
        .ok_or(MissingMessageSegment(segment.to_string()))?
        .field(field))
}

pub(crate) fn parse_field_value(
    msg: &Message,
    segment: &str,
    field: usize,
) -> Result<Option<String>, MessageAccessError> {
    Ok(parse_field(msg, segment, field)?.and_then(|f| {
        if f.is_empty() {
            None
        } else {
            Some(f.raw_value().to_string())
        }
    }))
}

pub(crate) fn parse_repeating_field<'a>(
    msg: &'a Message,
    segment: &str,
    field: usize,
) -> Result<Option<Vec<Repeat<'a>>>, MessageAccessError> {
    Ok(parse_field(msg, segment, field)?.and_then(|f| {
        if f.is_empty() {
            None
        } else {
            Some(f.repeats.clone())
        }
    }))
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

    segment
        .fields()
        .nth(field_number - 1)?
        .repeats()
        .nth(repeat_index)?
        .component(component_number)
        .filter(|c| !c.is_empty())
        .map(|v| v.raw_value().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FallConfig, Fhir, PatientConfig};
    use crate::fhir::mapper::Identifier;
    use crate::fhir::mapper::{
        FhirMapper, get_repeat_value, parse_component, parse_datetime, parse_subcomponents,
        patch_bundle_entry,
    };
    use crate::fhir::resources::{Department, ResourceMap};
    use crate::tests::read_test_resource;
    use fhir_model::DateTime::DateTime;
    use fhir_model::r4b::codes::HTTPVerb::Patch;
    use fhir_model::r4b::resources::{
        Bundle, BundleEntry, BundleEntryRequest, Encounter, Parameters, Patient, Resource,
        ResourceType,
    };

    use fhir_model::time::{Month, OffsetDateTime, Time};
    use fhir_model::{WrongResourceType, time};
    use hl7_parser::Message;
    use hl7_parser::parser::parse_field;
    use rstest::rstest;
    use std::collections::HashMap;

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
            .filter_map(|e| resource_from(e).ok())
            .collect();
        let encounter: Vec<Encounter> = bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| resource_from(e).ok())
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

    fn resource_from<T: TryFrom<Resource, Error = WrongResourceType>>(
        e: &BundleEntry,
    ) -> Result<T, WrongResourceType> {
        let r = e.resource.clone().unwrap();
        T::try_from(r)
    }

    #[test]
    fn test_parse_component() {
        let comp = parse_component(
            &parse_field("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L").unwrap(),
            3,
        );

        assert_eq!(comp, Some("Holzhausen".to_string()))
    }

    #[test]
    fn test_parse_subcomponent() {
        let sub = parse_subcomponents(
            parse_field("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L")
                .unwrap()
                .repeat(1)
                .unwrap(),
            1,
        );

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

        assert_eq!(result.as_deref(), expected);
    }

    #[test]
    fn test_parse_repeating_field() {
        let segment_raw = r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
IN1|2||777777777^^^^NII~BG HM HAUPT^^^^XX|BGHM - Hauptverwaltung|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||000000000001^PRN^PH^^^0800^99900801^^^^^000000000001~1313131331313^PRN^FX^^^00000^00000000^^^^^1313131331313||Träger der ges. Unfallversicherer^26^^^2&Berufsgenossenschaft^^NII~Träger der ges. Unfallversicherer^26^^^^^U||||||10001|Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||H|||||||||M||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L"#;
        let msg = Message::parse_with_lenient_newlines(segment_raw, true).unwrap();

        let result = parse_repeating_field(&msg, "IN1", 3).unwrap().unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(
            result.iter().map(|r| r.raw_value()).collect::<Vec<&str>>(),
            vec!["777777777^^^^NII", "BG HM HAUPT^^^^XX"]
        );
    }
}
