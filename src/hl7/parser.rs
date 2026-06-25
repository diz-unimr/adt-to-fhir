use crate::error::MessageTypeError::MissingMessageType;
use crate::error::{MessageTypeError, ParsingError};
use crate::hl7::parser::MessageType::*;
use anyhow::anyhow;
use hl7_parser::Message;
use hl7_parser::message::{Repeat, Segment};
use hl7_parser::query::LocationQueryResult;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

/// old patient identifier value
///
/// __note:__ only used at correction of patient data (e.g. merge operation)
pub(crate) const MRG_1: &str = "MRG.1";

/// message key
///
/// __note:__ always present
pub(crate) const MSH_10: &str = "MSH.10";

/// patient identifier
///
/// __note:__ always present (preferred before PID.3)
pub(crate) const PID_2: &str = "PID.2";
/// patient identifier list
///
/// __note:__ always present
pub(crate) const PID_3_1: &str = "PID.3.1";
/// encounter identifier (medical case id)
///
pub(crate) const PID_4: &str = "PID.4";
/// patient name
///
/// PID.5.7 (L) legal name, (M) maiden name
/// __note:__ repeats and components inside
pub(crate) const PID_5: &str = "PID.5";
/// patient birthdate
pub(crate) const PID_7: &str = "PID.7";
/// patient gender
pub(crate) const PID_8: &str = "PID.8";
/// marital status
pub(crate) const PID_16_1: &str = "PID.16.1";

/// mothers encounter number
///
/// __note:__ only at birth context set
pub(crate) const PID_21_1: &str = "PID.21.1";
/// multiple birth indicator
pub(crate) const PID_24: &str = "PID.24";
/// Birth order
pub(crate) const PID_25: &str = "PID.25";
/// patient death datetime
pub(crate) const PID_29: &str = "PID.29";
/// patient death confirmation flag
pub(crate) const PID_30: &str = "PID.30";

/// patient class
///
/// inpatient(I), ambulatory(O), emergency (E)...
pub(crate) const PV1_2: &str = "PV1.2";
/// ward short name
///
/// __note:__ may be empty
pub(crate) const PV1_3_1: &str = "PV1.3.1";
/// patient location room
pub(crate) const PV1_3_2: &str = "PV1.3.2";
/// patient location bed number
pub(crate) const PV1_3_3: &str = "PV1.3.3";
/// department short name
///
/// __note:__ in rare cases empty (ambulatory bed status and ward visit)
pub(crate) const PV1_3_4: &str = "PV1.3.4";
/// based on message type this may be 'department' or 'private clinic department' or 'generic location'
///
/// __note:__ usually set
pub(crate) const PV1_3_5: &str = "PV1.3.5";
/// admission source
pub(crate) const PV1_4_1: &str = "PV1.4.1";
/// admission reason
///
/// digit 3 & 4
pub(crate) const PV1_4__2_1: &str = "PV1.4[2].1";
/// encounter number (medical case id)
///
/// __note:__ usually set, may be missing first messages at encounter planning
pub(crate) const PV1_19_1: &str = "PV1.19.1";
/// discharge reason
pub(crate) const PV1_36_1: &str = "PV1.36.1";
/// clinical department code (german §301 Fachabteilungsschlüssel)
///
/// __note:__ often set
pub(crate) const PV1_39_1: &str = "PV1.39.1";
/// discharge disposition
pub(crate) const PV1_40_1: &str = "PV1.40.1";
/// encounter beginn date time
pub(crate) const PV1_44: &str = "PV1.44";
/// encounter end date time
pub(crate) const PV1_45: &str = "PV1.45";

/// admission reason
///
/// digit 1 & 2
pub(crate) const PV2_3_1: &str = "PV2.3.1";

/// patient movement identifier
///
/// __note:__ mandatory present at most message types. Missing at message types: A28-A34, A40-A47
pub(crate) const ZBE_1_1: &str = "ZBE.1.1";
/// beginning of patient movement (timestamp)
///
/// __note:__ mandatory present at most message types. Missing at message types: A28-A34, A40-A47
pub(crate) const ZBE_2: &str = "ZBE.2.1";
/// end of patient movement (timestamp)
///
/// __note:__ mandatory present at most message types. Missing at message types: A28-A34, A40-A47
pub(crate) const ZBE_3: &str = "ZBE.3.1";

/// birth weight
///
/// __note:__ segment only at birth context present
pub(crate) const ZNG_7: &str = "ZNG.7";
/// head circumference at birth
///
/// __note:__ segment only at birth context present
pub(crate) const ZNG_11: &str = "ZNG.11";
/// body length at birth
///
/// __note:__ segment only at birth context present
pub(crate) const ZNG_6: &str = "ZNG.6";

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
    /// CancelAdmitOrVisit
    A11,
    /// CancelTransfer
    A12,
    /// CancelDischarge
    A13,
    /// PendingAdmit
    A14,
    /// Beginn patient on leave
    A21,
    /// End patient on leave
    A22,
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
    /// DeletePreAdmit
    A38,
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
            "A21" => Ok(A21),
            "A22" => Ok(A22),
            "A27" => Ok(A27),
            "A28" => Ok(A28),
            "A29" => Ok(A29),
            "A31" => Ok(A31),
            "A34" => Ok(A34),
            "A38" => Ok(A38),
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

pub(crate) fn get_message_key<'a>(msg: &'a Message<'_>) -> Result<&'a str, ParsingError> {
    query(msg, MSH_10).ok_or(ParsingError::Other(anyhow!("failed to parse message key")))
}

pub(crate) fn check_is_numeric_ascii(input: &str, source: &str) -> Result<bool, ParsingError> {
    if !input.is_empty() && input.chars().all(|c| c.is_ascii_digit()) {
        Ok(true)
    } else {
        Err(ParsingError::Other(anyhow!(
            "input '{}' should be numeric but got '{}'",
            source,
            input
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::tests::read_test_resource;
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

        let v1 = query(&msg, PV1_3_1).unwrap();
        assert_eq!("WARD_1", v1);
        let v2 = query(&msg, PV1_3_2).unwrap();
        assert_eq!("room_1", v2);
        let v3 = query(&msg, PV1_3_3).unwrap();
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

        let v1 = query(&msg, PV1_3_1);
        assert_eq!(None, v1);
    }

    #[test]
    fn test_get_message_key() {
        let hl7 = read_test_resource("a04_test.hl7");
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");

        assert!(matches!(get_message_key(&msg), Ok("103601138")));
    }

    #[test]
    fn test_get_message_key_failed() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^DUMMY||P|2.5|||NE|NE||8859/1
EVN|A01|202111221030|202111221029||
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).expect("parse hl7 failed");

        assert!(matches!(get_message_key(&msg), Err(ParsingError::Other(_))));
    }
    #[test]
    fn check_is_numeric_ascii_test() {
        assert!(check_is_numeric_ascii("01", "test").unwrap());

        if let Err(ParsingError::Other(_)) = check_is_numeric_ascii("", "test-empty") {
        } else {
            panic!("check_is_numeric_ascii failed for empty input");
        }

        if let Err(ParsingError::Other(_)) = check_is_numeric_ascii("a", "test-empty") {
        } else {
            panic!("check_is_numeric_ascii failed for alphanumeric input");
        }
    }
}
