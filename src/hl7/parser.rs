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
    /// Admit
    A01,
    /// Transfer
    A02,
    /// Discharge
    A03,
    /// Registration
    A04,
    /// PreAdmit
    A05,
    /// ChangeOutpatientToInpatient
    A06,
    /// ChangeInpatientToOutpatient
    A07,
    /// PatientUpdate
    A08,
    /// CancelAdmitVisit
    A11,
    /// CancelTransfer
    A12,
    /// CancelDischarge
    A13,
    /// PendingAdmit
    A14,
    /// CancelPendingAdmit
    A27,
    /// AddPersonInformation
    A28,
    /// DeletePersonInformation
    A29,
    /// ChangePersonData
    A31,
    /// PatientMerge
    A34,
    /// MergePatientRecords
    A40,
    /// PatientReassignmentToSingleCase
    A45,
    /// PatientReassignmentToAllCases
    A47,
    /// UpdateEncounterNumber
    A50,
}

impl Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for MessageType {
    type Err = MessageTypeError;

    fn from_str(s: &str) -> Result<Self, MessageTypeError> {
        match s {
            "A01" => Ok(A01),
            "A02" => Ok(A02),
            "A03" => Ok(A03),
            "A04" => Ok(A04),
            "A05" => Ok(A05),
            "A06" => Ok(A06),
            "A07" => Ok(A07),
            "A08" => Ok(A08),
            "A11" => Ok(A11),
            "A12" => Ok(A12),
            "A13" => Ok(A13),
            "A14" => Ok(A14),
            "A27" => Ok(A27),
            "A28" => Ok(A28),
            "A29" => Ok(A29),
            "A31" => Ok(A31),
            "A34" => Ok(A34),
            "A40" => Ok(A40),
            "A45" => Ok(A45),
            "A47" => Ok(A47),
            "A50" => Ok(A50),
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

pub(crate) fn parse_field_value<'a>(
    msg: &'a Message,
    segment: &str,
    field: usize,
) -> Result<Option<&'a str>, MessageAccessError> {
    Ok(parse_field(msg, segment, field)?.and_then(|f| {
        if f.is_empty() {
            None
        } else {
            Some(f.raw_value())
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

pub(crate) fn parse_repeating_field_component_value<'a>(
    msg: &'a Message<'a>,
    segment: &str,
    field: usize,
    component: usize,
) -> Result<Option<String>, MessageAccessError> {
    let f_extracted = msg
        .segment(segment)
        .ok_or(MissingMessageSegment(segment.to_string()))?;
    Ok(get_repeat_value(f_extracted, field, 0, component))
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
    #[test]
    fn test_parse_repeating_field_component_value() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.3|||||D||DE
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|I|WARD_1^room_1^bed_1^KJM^KLINIKUM^123445|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331
"#;
        let msg = Message::parse_with_lenient_newlines(&input, true).expect("parse hl7 failed");

        let v1 = parse_repeating_field_component_value(&msg, "PV1", 3, 1)
            .expect("parse hl7 failed")
            .unwrap();
        assert_eq!("WARD_1".to_string(), v1);
        let v2 = parse_repeating_field_component_value(&msg, "PV1", 3, 2)
            .expect("parse hl7 failed")
            .unwrap();
        assert_eq!("room_1".to_string(), v2);
        let v3 = parse_repeating_field_component_value(&msg, "PV1", 3, 3)
            .expect("parse hl7 failed")
            .unwrap();
        assert_eq!("bed_1".to_string(), v3);
    }

    #[test]
    fn test_parse_repeating_field_component_value_empty() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.3|||||D||DE
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|I|^^^KJM^KLINIKUM^123445|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331
"#;
        let msg = Message::parse_with_lenient_newlines(&input, true).expect("parse hl7 failed");

        let v1 =
            parse_repeating_field_component_value(&msg, "PV1", 3, 1).expect("parse hl7 failed");
        assert_eq!(None, v1);
    }

    #[test]
    fn test_parse_repeating_field_component_value_segment_missing() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.3|||||D||DE
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
"#;
        let msg = Message::parse_with_lenient_newlines(&input, true).expect("parse hl7 failed");
        let _actual = parse_repeating_field_component_value(&msg, "PV1", 3, 1).expect_err("PV1");
    }
}
