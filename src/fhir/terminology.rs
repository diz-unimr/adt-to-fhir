// todo: generate terminology from FHIR models
use fhir_model::r4b::types::Coding;

pub(crate) enum AufnahmeGrundStelle<'a> {
    ErsteUndZweite(&'a str),
    Dritte(&'a str),
    Vierte(&'a str),
}

impl From<AufnahmeGrundStelle<'_>> for Option<Coding> {
    fn from(value: AufnahmeGrundStelle) -> Self {
        match value {
            AufnahmeGrundStelle::ErsteUndZweite(code) => {
                aufnahmegrund_erste_und_zweite_stelle(code)
            }
            AufnahmeGrundStelle::Dritte(code) => aufnahmegrund_dritte_stelle(code),
            AufnahmeGrundStelle::Vierte(code) => aufnahmegrund_vierte_stelle(code),
        }
    }
}

pub(crate) enum EntlassgrundStelle<'a> {
    ErsteUndZweite(&'a str),
    Dritte(&'a str),
}

impl From<EntlassgrundStelle<'_>> for Option<Coding> {
    fn from(value: EntlassgrundStelle) -> Self {
        match value {
            EntlassgrundStelle::ErsteUndZweite(code) => entlassgrund_erste_und_zweite_stelle(code),
            EntlassgrundStelle::Dritte(code) => entlassgrund_dritte_stelle(code),
        }
    }
}

fn aufnahmegrund_erste_und_zweite_stelle(code: &str) -> Option<Coding> {
    let display = match code {
        "01" => "Krankenhausbehandlung, vollstationär",
        "02" => {
            "Krankenhausbehandlung, vollstationär mit vorausgegangener vorstationärer Behandlung"
        }
        "03" => "Krankenhausbehandlung, teilstationär",
        "04" => "vorstationäre Behandlung ohne anschließende vollstationäre Behandlung",
        "05" => "Stationäre Entbindung",
        "06" => "Geburt",
        "07" => "Wiederaufnahme wegen Komplikationen (Fallpauschale) nach KFPV 2003",
        "08" => "Stationäre Aufnahme zur Organentnahme",
        "10" => "Stationsäquivalente Behandlung",
        _ => return None,
    };

    Coding::builder()
        .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle".to_string())
        .code(code.to_string())
        .display(display.to_string())
        .build()
        .ok()
}

fn aufnahmegrund_dritte_stelle(code: &str) -> Option<Coding> {
    let display = match code {
        "0" => "Anderes",
        "2" => "Zuständigkeitswechsel des Kostenträgers",
        "4" => "Behandlungen im Rahmen von Verträgen zur integrierten Versorgung",
        _ => return None,
    };

    Coding::builder()
        .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundDritteStelle".to_string())
        .code(code.to_string())
        .display(display.to_string())
        .build()
        .ok()
}

fn aufnahmegrund_vierte_stelle(code: &str) -> Option<Coding> {
    let display = match code {
        "1" => "Normalfall",
        "2" => "Arbeitsunfall/Berufskrankheit (§ 11 Abs. 5 SGB V)",
        "3" => "Verkehrsunfall/Sportunfall/Sonstiger Unfall (z.B. § 116 SGB X)",
        "4" => "Hinweis auf Einwirkung von äußerer Gewalt",
        "6" => "Kriegsbeschädigten-Leiden/BVG-Leiden",
        "7" => "Notfall",
        _ => return None,
    };

    Coding::builder()
        .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundVierteStelle".to_string())
        .code(code.to_string())
        .display(display.to_string())
        .build()
        .ok()
}

fn entlassgrund_erste_und_zweite_stelle(code: &str) -> Option<Coding> {
    let display = match code {
        "01" => "Behandlung regulär beendet",
        "02" => "Behandlung regulär beendet, nachstationäre Behandlung vorgesehen",
        "03" => "Behandlung aus sonstigen Gründen beendet",
        "04" => "Behandlung gegen ärztlichen Rat beendet",
        "05" => "Zuständigkeitswechsel des Kostenträgers",
        "06" => "Verlegung in ein anderes Krankenhaus",
        "07" => "Tod",
        "08" => {
            "Verlegung in ein anderes Krankenhaus im Rahmen einer Zusammenarbeit (§ 14 Abs. 5 Satz 2 BPflV in der am 31.12.2003 geltenden Fassung)"
        }
        "09" => "Entlassung in eine Rehabilitationseinrichtung",
        "10" => "Entlassung in eine Pflegeeinrichtung",
        "11" => "Entlassung in ein Hospiz",
        "12" => "interne Verlegung, arbeitsfähig entlassen",
        "13" => "externe Verlegung zur psychiatrischen Behandlung",
        "14" => "Behandlung aus sonstigen Gründen beendet, nachstationäre Behandlung vorgesehen",
        "15" => "Behandlung gegen ärztlichen Rat beendet, nachstationäre Behandlung vorgesehen",
        "16" => {
            "externe Verlegung mit Rückverlegung oder Wechsel zwischen den Entgeltbereichen der DRG-Fallpauschalen, nach der BPflV oder für besondere Einrichtungen nach § 17b Abs. 1 Satz 15 KHG mit Rückverlegung"
        }
        "17" => {
            "interne Verlegung mit Wechsel zwischen den Entgeltbereichen der DRG-Fallpauschalen, nach der BPflV oder für besondere Einrichtungen nach § 17b Abs. 1 Satz 15 KHG"
        }
        "18" => "Rückverlegung",
        "19" => "Entlassung vor Wiederaufnahme mit Neueinstufung",
        "20" => "Entlassung vor Wiederaufnahme mit Neueinstufung wegen Komplikation",
        "21" => "Entlassung oder Verlegung mit nachfolgender Wiederaufnahme",
        "22" => {
            "Fallabschluss (interne Verlegung) bei Wechsel zwischen voll-, teilstationärer und stationsäquivalenter Behandlung"
        }
        "23" => {
            "Beginn eines externen Aufenthalts mit Abwesenheit über Mitternacht (BPflV-Bereich – für verlegende Fachabteilung)"
        }
        "24" => {
            "Beendigung eines externen Aufenthalts mit Abwesenheit über Mitternacht (BPflV-Bereich – für Pseudo-Fachabteilung 0003)"
        }
        "25" => {
            "Entlassung zum Jahresende bei Aufnahme im Vorjahr (für Zwecke der Abrechnung - § 4 PEPPV)"
        }
        "26" => {
            "Beginn eines Zeitraumes ohne direkten Patientenkontakt (stationsäquivalente Behandlung)"
        }
        "27" => {
            "Beendigung eines Zeitraumes ohne direkten Patientenkontakt (stationsäquivalente Behandlung – für Pseudo-Fachabteilung 0004)"
        }
        "28" => "Behandlung regulär beendet, beatmet entlassen",
        "29" => "Behandlung regulär beendet, beatmet verlegt",
        _ => return None,
    };

    Coding::builder()
        .system("http://fhir.de/CodeSystem/dkgev/EntlassungsgrundErsteUndZweiteStelle".to_string())
        .code(code.to_string())
        .display(display.to_string())
        .build()
        .ok()
}

fn entlassgrund_dritte_stelle(code: &str) -> Option<Coding> {
    let display = match code {
        "1" => "arbeitsfähig entlassen",
        "2" => "arbeitsunfähig entlassen",
        "9" => "keine Angabe",
        _ => return None,
    };

    Coding::builder()
        .system("http://fhir.de/CodeSystem/dkgev/EntlassungsgrundDritteStelle".to_string())
        .code(code.to_string())
        .display(display.to_string())
        .build()
        .ok()
}

pub fn diagnose_role_coding(code: &str) -> Option<Coding> {
    let display = match code {
        "AD" => "Admission diagnosis",
        "DD" => "Discharge diagnosis",
        "CC" => "Chief complaint",
        "CM" => "Comorbidity diagnosis",
        "pre-op" => "pre-op diagnosis",
        "post-op" => "post-op diagnosis",
        "billing" => "Billing",
        _ => return None,
    };

    Coding::builder()
        .system("http://terminology.hl7.org/CodeSystem/diagnosis-role".to_string())
        .code(code.to_string())
        .display(display.to_string())
        .build()
        .ok()
}

pub fn kontakt_diagnose_procedures(code: &str) -> Option<Coding> {
    let display = match code {
        "referral-diagnosis" => "Überweisungsdiagnose",
        "treatment-diagnosis" => "Behandlungsrelevante Diagnosen",
        "hospital-main-diagnosis" => "Krankenhaus Hauptdiagnose",
        "surgery-diagnosis" => "Operationsdiagnose",
        "principle-DRG" => "DRG-Hauptdiagnose",
        "secondary-DRG" => "DRG-Nebendiagnose",
        "department-main-diagnosis" => "Abteilung Hauptdiagnose",
        "infection-control-diagnosis" => "Infektionsschutzdiagnose",
        "cause-of-death" => "Todesursache",
        _ => return None,
    };

    Coding::builder()
        .system("http://fhir.de/CodeSystem/KontaktDiagnoseProzedur".to_string())
        .code(code.to_string())
        .display(display.to_string())
        .build()
        .ok()
}
