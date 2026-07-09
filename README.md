# adt-to-fhir

[![MegaLinter](https://github.com/diz-unimr/adt-to-fhir/actions/workflows/mega-linter.yml/badge.svg)](https://github.com/diz-unimr/adt-to-fhir/actions/workflows/mega-linter.yml)
[![build](https://github.com/diz-unimr/adt-to-fhir/actions/workflows/build.yaml/badge.svg)](https://github.com/diz-unimr/adt-to-fhir/actions/workflows/build.yaml)
[![docker](https://github.com/diz-unimr/adt-to-fhir/actions/workflows/release.yaml/badge.svg)](https://github.com/diz-unimr/adt-to-fhir/actions/workflows/release.yaml)
[![codecov](https://codecov.io/gh/diz-unimr/adt-to-fhir/graph/badge.svg?token=urFEEfhEJB)](https://codecov.io/gh/diz-unimr/adt-to-fhir)


> HL7 ADT to FHIR 🔥 Kafka processor

## Offset handling

The consumer is configured to auto-commit offsets, in order to improve performance. Successfully processed (and
produced) records are committed manually to the offset store (`enable.auto.offset.store`) to be eligible for
auto-commiting.

## Mapping

> [!WARNING]  
> TODO

## Configuration properties

Application properties are read from a properties file ([app.yaml](./app.yaml)) with default values.

| Name                                          | Default                                                                                                                        | Description                                                                                                               |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `app.log_level`                               | info                                                                                                                           | Log level (error,warn,info,debug,trace)                                                                                   |
| `app.telemetry_endpoint`                      | http://localhost:4317                                                                                                          |                                                                                                                           | 
| `kafka.brokers`                               | localhost:9092                                                                                                                 | Kafka brokers                                                                                                             |
| `kafka.security_protocol`                     | plaintext                                                                                                                      | Kafka communication protocol                                                                                              |
| `kafka.ssl.ca_location`                       | /app/cert/kafka_ca.pem                                                                                                         | Kafka CA certificate location                                                                                             |
| `kafka.ssl.certificate_location`              | /app/cert/app_cert.pem                                                                                                         | Client certificate location                                                                                               |
| `kafka.ssl.key_location`                      | /app/cert/app_key.pem                                                                                                          | Client key location                                                                                                       |
| `kafka.ssl.key_password`                      |                                                                                                                                | Client key password                                                                                                       |
| `kafka.consumer_group`                        | adt-to-fhir                                                                                                                    | Consumer group name                                                                                                       |
| `kafka.input_topic`                           | adt-hl7                                                                                                                        | Kafka topic to consume                                                                                                    |
| `kafka.output_topic`                          | adt-fhir                                                                                                                       | Kafka output topic                                                                                                        |
| `kafka.offset_reset`                          | earliest                                                                                                                       | Kafka consumer reset (`earliest` or `latest`)                                                                             |
| `fhir.meta_source`                            | "#orbis_adt"                                                                                                                   | Value of `resource.meta.source`                                                                                           |  
| `fhir.bundle_identifier_system`               | `https://fhir.diz.uni-marburg.de/sid/bundle-id`                                                                                | `Bundle` identifier system                                                                                                |
| `fhir.check_mode`                             | strict                                                                                                                         | Führt eine fehlender Mapping Eintrag zu einem Verarbeitungs-Stop. Zulässige Werte sind: _strict_, _lenienet_              |
| `fhir.person.profile`                         | `https://www.medizininformatik-initiative.de/fhir/core/modul-person/StructureDefinition/Patient\|2026.0.0`                     | `Patient` FHIR profile                                                                                                    |
| `fhir.person.system`                          | `https://fhir.diz.uni-marburg.de/sid/patient-id`                                                                               | `Patient` identifier system                                                                                               |
| `fhir.person.other_insurance_system`          | `https://fhir.diz.uni-marburg.de/sid/patient-other-insurance-id`                                                               | `Patient` identifier system for legazy insurance numbers which defer from current regulations and FHIR profile definition |
| `fhir.fall.profile`                           | `https://www.medizininformatik-initiative.de/fhir/core/modul-fall/StructureDefinition/KontaktGesundheitseinrichtung\|2026.0.0` | `Encounter` FHIR profile                                                                                                  |
| `fhir.fall.system`                            | `https://fhir.diz.uni-marburg.de/sid/encounter-id`                                                                             | `Encounter` identifier system                                                                                             | 
| `fhir.fall.einrichtungskontakt.system`        | `https://fhir.diz.uni-marburg.de/sid/encounter-admit-id`                                                                       | `Encounter` (_Einrichtungskontakt_) identifier system                                                                     |
| `fhir.fall.abteilungskontakt.system`          | `https://fhir.diz.uni-marburg.de/sid/encounter-department-id`                                                                  | `Encounter` (_Abteilungskontakt_) identifier system                                                                       |
| `fhir.fall.versorgungsstellenkontakt.system`  | `https://fhir.diz.uni-marburg.de/sid/encounter-caresite-id`                                                                    | `Encounter` (_Versorgungsstellenkontakt_) identifier system                                                               |
| `fhir.location.system_ward`                   | `https://fhir.diz.uni-marburg.de/sid/location-caresite-id`                                                                     | `Location` (_Station_) identifier system                                                                                  |   
| `fhir.location.system_room`                   | `https://fhir.diz.uni-marburg.de/sid/location-room-id`                                                                         | `Location` (_Zimmer Kennung_) identifier system                                                                           |
| `fhir.location.system_bed`                    | `https://fhir.diz.uni-marburg.de/sid/location-bed-id`                                                                          | `Location` (_Bett Kennung_) identifier system                                                                             |
| `fhir.condition.system`                       | `https://fhir.diz.uni-marburg.de/sid/condition-id`                                                                             | `Condition` (_Diagnose_) identifier system                                                                                |
| `fhir.observation.system`                     | `https://fhir.diz.uni-marburg.de/sid/observation-id`                                                                           | `Observation` identifier system                                                                                           |
| `fhir.observation.profile_head_circumference` | `https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/mii-pr-icu-kopfumfang\|2025.0.4`           | Köpfumfang FHIR Profil                                                                                                    |
| `fhir.observation.profile_weight`             | `https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/mii-pr-icu-muv-koerpergewicht\|2025.0.4`   | Körpergewicht (_bei Geburt_) FHIR Profil                                                                                  |
| `fhir.observation.profile_vital_status`       | `https://www.medizininformatik-initiative.de/fhir/core/modul-person/StructureDefinition/Vitalstatus\|2025.0.4`                 | Vital Status (_bei Aufnahme, Verlegung, Entlassung_) FHIR Profil                                                          |
| `fhir.observation.profile_height`             | `https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/mii-pr-icu-muv-koerpergroesse\|2025.0.4`   | Körpergröße (_bei Geburt_) FHIR Profil                                                                                    |
| `fhir.organization.department.system`         | `https://fhir.diz.uni-marburg.de/sid/department`                                                                               | `Organization` (_Fachabteilung_) identifier system                                                                        |
| `fhir.organization.ward.system`               | `https://fhir.diz.uni-marburg.de/sid/ward-id`                                                                                  | `Organization` (_Station_) identifier system                                                                              |

### Resource files

#### InfoByAbteilungskuerzel.json

We do not want false mapping result to 'unknown' by
default. Therefore, we expect every department to have a valid entry. If
department has no valid department identification code you may map `3700` value.
Missing entries will result in mapping error and processing will stop, if `fhir.check_mode` is set to `strict`,
while `lenient` configuration will fall back to `3700`.

Please not, department name `abteilungsBezeichnung` at this mapping is a __lokal name__, official medical department
name will be mapped via department id from `Fachabteilungsschluessel-erweitert`(see section below).
The lokal name will be used to create department organization resources.

Content format is like th following example, _department short name_ must be unique.

```json
{
  "department short name": {
    "abteilungsBezeichnung": "lokal department name",
    "fachabteilungsSchluessel": "department id as defined at §301"
  }
}
```

#### InfoStation.json

This mapping file is used to define which wards are used as _intensive care units_ and in which time periods they are
assigned this status.

Content format is like th following example, _ward short name_ used at _HL7v2_ field `PV1-3.1`must be unique.
Properties _validTo_ (if empty no expiration date is assumed) and _isIcu_ (if empty _false_ is assumed) are optional
values.

```json
{
  "WARD42": {
    "display": "Ward 42",
    "isIcu": false,
    "validPeriod": [
      {
        "validFrom": "2000-02-02"
      },
      {
        "validFrom": "1984-02-02",
        "validTo": "1990-02-01"
      }
    ]
  }
}
```

#### Department id code system

In Germany most medical departments can be assigned a general department id _Fachabteilungsschlüssel_, the whole list
is available via http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert and must be mapped as volume into
your docker container. We support JSON format.

This entries will be used to map encounter of second level (*Abteilungskontakt*) at property
`encounter.serviceType.coding`.

### Environment variables

Override configuration properties by providing environment variables with their respective property names.

## Deployment

Example files for deployment can be found in dictionary _deploy_.

## License

[AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html)
