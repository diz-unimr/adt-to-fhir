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

pub(crate) fn aufnahmegrund_erste_und_zweite_stelle(value: &str) -> Option<Coding> {
    match value {
        "01" => Coding::builder()
            .system(
                "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                    .to_string(),
            )
            .code("01".to_string())
            .display("Krankenhausbehandlung, vollstationär".to_string())
            .build().ok(),
        "02" =>
            Coding::builder()
                .system(
                    "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                        .to_string(),
                )
                .code("02".to_string())
                .display("Krankenhausbehandlung, vollstationär mit vorausgegangener vorstationärer Behandlung".to_string())
                .build().ok(),
        "03" =>
            Coding::builder()
                .system(
                    "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                        .to_string(),
                )
                .code("03".to_string())
                .display("Krankenhausbehandlung, teilstationär".to_string())
                .build().ok(),
        "04" =>
            Coding::builder()
                .system(
                    "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                        .to_string(),
                )
                .code("04".to_string())
                .display("vorstationäre Behandlung ohne anschließende vollstationäre Behandlung".to_string())
                .build().ok(),
        "05" =>
            Coding::builder()
                .system(
                    "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                        .to_string(),
                )
                .code("05".to_string())
                .display("Stationäre Entbindung".to_string())
                .build().ok(),
        "06" =>
            Coding::builder()
                .system(
                    "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                        .to_string(),
                )
                .code("06".to_string())
                .display("Geburt".to_string())
                .build().ok(),
        "07" =>
            Coding::builder()
                .system(
                    "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                        .to_string(),
                )
                .code("07".to_string())
                .display("Wiederaufnahme wegen Komplikationen (Fallpauschale) nach KFPV 2003".to_string())
                .build().ok(),
        "08" =>
            Coding::builder()
                .system(
                    "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                        .to_string(),
                )
                .code("08".to_string())
                .display("Stationäre Aufnahme zur Organentnahme".to_string())
                .build().ok(),
        "10" => Coding::builder()
            .system(
                "http://fhir.de/CodeSystem/dkgev/AufnahmegrundErsteUndZweiteStelle"
                    .to_string(),
            )
            .code("10".to_string())
            .display("Stationsäquivalente Behandlung".to_string())
            .build().ok(),
        _ => None
    }
}

pub(crate) fn aufnahmegrund_dritte_stelle(code: &str) -> Option<Coding> {
    match code {
        "0" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundDritteStelle".to_string())
            .code("0".to_string())
            .display("Anderes".to_string())
            .build()
            .ok(),
        "2" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundDritteStelle".to_string())
            .code("2".to_string())
            .display("Zuständigkeitswechsel des Kostenträgers".to_string())
            .build()
            .ok(),
        "4" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundDritteStelle".to_string())
            .code("4".to_string())
            .display("Behandlungen im Rahmen von Verträgen zur integrierten Versorgung".to_string())
            .build()
            .ok(),
        _ => None,
    }
}

pub(crate) fn aufnahmegrund_vierte_stelle(code: &str) -> Option<Coding> {
    match code {
        "1" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundVierteStelle".to_string())
            .code("1".to_string())
            .display("Normalfall".to_string())
            .build()
            .ok(),
        "2" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundVierteStelle".to_string())
            .code("2".to_string())
            .display("Arbeitsunfall/Berufskrankheit (§ 11 Abs. 5 SGB V)".to_string())
            .build()
            .ok(),
        "3" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundVierteStelle".to_string())
            .code("3".to_string())
            .display("Verkehrsunfall/Sportunfall/Sonstiger Unfall (z.B. § 116 SGB X)".to_string())
            .build()
            .ok(),
        "4" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundVierteStelle".to_string())
            .code("4".to_string())
            .display("Hinweis auf Einwirkung von äußerer Gewalt".to_string())
            .build()
            .ok(),
        "6" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundVierteStelle".to_string())
            .code("6".to_string())
            .display("Hinweis auf Einwirkung von äußerer Gewalt".to_string())
            .build()
            .ok(),
        "7" => Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/AufnahmegrundVierteStelle".to_string())
            .code("7".to_string())
            .display("Notfall".to_string())
            .build()
            .ok(),
        _ => None,
    }
}
