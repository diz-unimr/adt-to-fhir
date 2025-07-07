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

| Name                                         | Default                                                                                                                        | Description                                                 |
|----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| `app.log_level`                              | info                                                                                                                           | Log level (error,warn,info,debug,trace)                     |
| `kafka.brokers`                              | localhost:9092                                                                                                                 | Kafka brokers                                               |
| `kafka.security_protocol`                    | plaintext                                                                                                                      | Kafka communication protocol                                |
| `kafka.ssl.ca_location`                      | /app/cert/kafka_ca.pem                                                                                                         | Kafka CA certificate location                               |
| `kafka.ssl.certificate_location`             | /app/cert/app_cert.pem                                                                                                         | Client certificate location                                 |
| `kafka.ssl.key_location`                     | /app/cert/app_key.pem                                                                                                          | Client key location                                         |
| `kafka.ssl.key_password`                     |                                                                                                                                | Client key password                                         |
| `kafka.consumer_group`                       | adt-to-fhir                                                                                                                    | Consumer group name                                         |
| `kafka.input_topic`                          | adt-hl7                                                                                                                        | Kafka topic to consume                                      |
| `kafka.output_topic`                         | adt-fhir                                                                                                                       | Kafka output topic                                          |
| `kafka.offset_reset`                         | earliest                                                                                                                       | Kafka consumer reset (`earliest` or `latest`)               |
| `fhir.person.profile`                        | `https://www.medizininformatik-initiative.de/fhir/core/modul-person/StructureDefinition/Patient\|2025.0.0`                     | `Patient` FHIR profile                                      |
| `fhir.person.system`                         | `https://fhir.diz.uni-marburg.de/sid/patient-id`                                                                               | `Patient` identifier system                                 |
| `fhir.fall.profile`                          | `https://www.medizininformatik-initiative.de/fhir/core/modul-fall/StructureDefinition/KontaktGesundheitseinrichtung\|2025.0.0` | `Encounter` FHIR profile                                    |
| `fhir.fall.system`                           | `https://fhir.diz.uni-marburg.de/sid/encounter-id`                                                                             | `Encounter` identifier system                               |
| `fhir.fall.einrichtungskontakt.system`       | `https://fhir.diz.uni-marburg.de/sid/encounter-admit-id`                                                                       | `Encounter` (_Einrichtungskontakt_) identifier system       |
| `fhir.fall.abteilungskontakt.system`         | `https://fhir.diz.uni-marburg.de/sid/encounter-department-id`                                                                  | `Encounter` (_Abteilungskontakt_) identifier system         |
| `fhir.fall.versorgungsstellenkontakt.system` | `https://fhir.diz.uni-marburg.de/sid/encounter-caresite-id`                                                                    | `Encounter` (_Versorgungsstellenkontakt_) identifier system |
| ...                                          | ...                                                                                                                            | ...                                                         |

### Environment variables

Override configuration properties by providing environment variables with their respective property names.

## License

[AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html)
