use fhir_model::r4b::types::Coding;
use std::collections::HashMap;
use std::sync::LazyLock;

pub(crate) static DIAGNOSE_ROLE_MAP: LazyLock<HashMap<String, Coding>> = LazyLock::new(|| {
    let system = "http://terminology.hl7.org/CodeSystem/diagnosis-role".to_string();
    let mut map = HashMap::new();
    map.insert(
        "AD".to_string(),
        Coding::builder()
            .code("AD".to_string())
            .system(system.clone())
            .display("Admission diagnosis".to_string())
            .build()
            .unwrap(),
    );
    map.insert(
        "DD".to_string(),
        Coding::builder()
            .code("DD".to_string())
            .system(system.clone())
            .display("Discharge diagnosis".to_string())
            .build()
            .unwrap(),
    );
    map.insert(
        "CC".to_string(),
        Coding::builder()
            .code("CC".to_string())
            .system(system.clone())
            .display("Chief complaint	".to_string())
            .build()
            .unwrap(),
    );
    map.insert(
        "CM".to_string(),
        Coding::builder()
            .code("CM".to_string())
            .system(system.clone())
            .display("Comorbidity diagnosis".to_string())
            .build()
            .unwrap(),
    );
    map.insert(
        "pre-op".to_string(),
        Coding::builder()
            .code("pre-op".to_string())
            .system(system.clone())
            .display("pre-op diagnosis".to_string())
            .build()
            .unwrap(),
    );
    map.insert(
        "post-op".to_string(),
        Coding::builder()
            .code("post-op".to_string())
            .system(system.clone())
            .display("post-op diagnosis".to_string())
            .build()
            .unwrap(),
    );

    map.insert(
        "billing".to_string(),
        Coding::builder()
            .code("billing".to_string())
            .system(system.clone())
            .display("Billing".to_string())
            .build()
            .unwrap(),
    );
    map
});
