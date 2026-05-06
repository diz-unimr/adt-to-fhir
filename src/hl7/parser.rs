use crate::error::MessageTypeError;
use crate::error::MessageTypeError::MissingMessageType;
use crate::hl7::parser::MessageType::*;
use hl7_parser::Message;
use hl7_parser::message::{Repeat, Segment};
use hl7_parser::query::LocationQueryResult;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

pub(crate) const ZNG_WEIGHT: &str = "ZNG.7";
pub(crate) const ZNG_HEAD_CIRCUMFERENCE: &str = "ZNG.11";
pub(crate) const ZNG_BODY_HEIGHT: &str = "ZNG.6";
pub(crate) const PID_PID: &str = "PID.3.1";
pub(crate) const PV1_VISIT_ID: &str = "PV1.19.1";
pub(crate) const ZBE_BEGINN_OF_MOVEMENT: &str = "ZBE.2";

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
/// Query message value by location.
///
/// # Examples
/// ```
/// let value = query(msg, "PID.1");
/// ```
/// [`None`] is returned if segments are empty or missing.
pub(crate) fn query<'a>(msg: &'a Message<'_>, location: &str) -> Option<&'a str> {
    msg.query(location)
        .map(|l| l.raw_value())
        .filter(|s| !s.is_empty())
}

/// Get component value of a repeating field.
///
/// Returns non-empty string slices ([`Option<&str>`]) or [`None`].
pub(crate) fn repeat_component<'a>(repeat: &Repeat<'a>, component: usize) -> Option<&'a str> {
    repeat
        .component(component)
        .map(|c| c.raw_value())
        .filter(|s| !s.is_empty())
}

/// Get subcomponent values of a repeating field.
///
/// Subcomponent values are non-empty string slices ([`Option<&str>`]) or [`None`].
pub(crate) fn repeat_subcomponents<'a>(
    repeat: &Repeat<'a>,
    component: usize,
) -> Option<Vec<&'a str>> {
    repeat.component(component).map(|c| {
        c.subcomponents
            .iter()
            .map(|s| s.raw_value())
            .filter(|s| !s.is_empty())
            .collect()
    })
}

/// Get field repeats of the provided query.
///
/// Returns an iterator of [`Repeat`], if query targets a [`Field`].
pub(crate) fn field_repeats<'a>(
    msg: &'a Message<'_>,
    query: &str,
) -> Option<impl Iterator<Item = &'a Repeat<'a>>> {
    match msg.query(query) {
        Some(LocationQueryResult::Field(f)) => Some(f.repeats()),
        _ => None,
    }
}

/// Extraktion eines Werts aus einem Segment
/// # Arguments
/// * `segment` - Referenz des Segments aus dem wir Informationen lesen wollen
/// * `field_number` - 1 basierter Feld-Index des Ziels
/// * `repeat_number` - 1 basierter Index der Feldwiederholungen
/// * `component_number` - 1 basierter Komponenten-Index des ausgewählten Repeats
/// # Result
/// `&str`-Wert des Eintrags. Sollte einer oder mehre Indexe außerhalb der verfügbaren Felder
/// liegen, so wird `None` zurückgeliefert.
///
pub(crate) fn segment_value<'a>(
    segment: &Segment<'a>,
    field_number: usize,
    repeat_number: usize,
    component_number: usize,
) -> Option<&'a str> {
    if field_number == 0 || repeat_number == 0 || component_number == 0 {
        return None;
    }

    segment
        .field(field_number)
        .and_then(|f| f.repeat(repeat_number))
        .and_then(|r| repeat_component(r, component_number))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hl7_parser::parser::parse_segment;
    use rstest::rstest;

    #[test]
    fn test_parse_subcomponent() {
        let sub = repeat_subcomponents(
            hl7_parser::parser::parse_field("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L")
                .unwrap()
                .repeat(1)
                .unwrap(),
            1,
        );

        assert_eq!(sub, Some(vec!["Talstraße 16", "Talstraße", "16"]))
    }

    #[rstest]
    #[case(3, 1, 1, Some("777777777"))]
    #[case(3, 1, 5, Some("NII"))]
    #[case(3, 2, 5, Some("XX"))]
    #[case(300, 1, 5, None)]
    #[case(3, 100, 5, None)]
    #[case(3, 2, 500, None)]
    #[case(0, 1, 0, None)]
    fn test_segment_value(
        #[case] field_number: usize,
        #[case] repeat_index: usize,
        #[case] component_number: usize,
        #[case] expected: Option<&str>,
    ) {
        let segment_raw = "IN1|2||777777777^^^^NII~BG HM HAUPT^^^^XX|BGHM - Hauptverwaltung|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||000000000001^PRN^PH^^^0800^99900801^^^^^000000000001~1313131331313^PRN^FX^^^00000^00000000^^^^^1313131331313||Träger der ges. Unfallversicherer^26^^^2&Berufsgenossenschaft^^NII~Träger der ges. Unfallversicherer^26^^^^^U||||||10001|Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||H|||||||||M||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L";

        let segment = parse_segment(segment_raw).unwrap();
        let result = segment_value(&segment, field_number, repeat_index, component_number);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_repeating_field() {
        let segment_raw = r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
IN1|2||777777777^^^^NII~BG HM HAUPT^^^^XX|BGHM - Hauptverwaltung|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||000000000001^PRN^PH^^^0800^99900801^^^^^000000000001~1313131331313^PRN^FX^^^00000^00000000^^^^^1313131331313||Träger der ges. Unfallversicherer^26^^^2&Berufsgenossenschaft^^NII~Träger der ges. Unfallversicherer^26^^^^^U||||||10001|Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||H|||||||||M||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L"#;
        let msg = Message::parse_with_lenient_newlines(segment_raw, true).unwrap();

        let result = field_repeats(&msg, "IN1.3").unwrap().collect::<Vec<_>>();

        assert_eq!(result.len(), 2);
        assert_eq!(
            result
                .into_iter()
                .map(|r| r.raw_value())
                .collect::<Vec<&str>>(),
            vec!["777777777^^^^NII", "BG HM HAUPT^^^^XX"]
        );
    }
    #[test]
    fn query_test() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.3|||||D||DE
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|I|WARD_1^room_1^bed_1^KJM^KLINIKUM^123445|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).expect("parse hl7 failed");

        let v1 = query(&msg, "PV1.3.1").unwrap();
        assert_eq!("WARD_1", v1);
        let v2 = query(&msg, "PV1.3.2").unwrap();
        assert_eq!("room_1", v2);
        let v3 = query(&msg, "PV1.3.3").unwrap();
        assert_eq!("bed_1", v3);
    }

    #[test]
    fn query_test_empty() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.3|||||D||DE
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|I|^^^KJM^KLINIKUM^123445|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).expect("parse hl7 failed");

        let v1 = query(&msg, "PV1.3.1");
        assert_eq!(None, v1);
    }
}
