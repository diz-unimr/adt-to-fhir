use crate::error::MessageAccessError::MissingMessageSegment;
use crate::error::MessageTypeError::MissingMessageType;
use crate::error::{MessageAccessError, MessageTypeError};
use crate::hl7::parser::MessageType::*;
use hl7_parser::Message;
use hl7_parser::message::{Field, Repeat, Segment};
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

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
    use rstest::rstest;

    #[test]
    fn test_parse_component() {
        let comp = parse_component(
            &hl7_parser::parser::parse_field("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L")
                .unwrap(),
            3,
        );

        assert_eq!(comp, Some("Holzhausen".to_string()))
    }

    #[test]
    fn test_parse_subcomponent() {
        let sub = parse_subcomponents(
            hl7_parser::parser::parse_field("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L")
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
