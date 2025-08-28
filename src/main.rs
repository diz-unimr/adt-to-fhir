mod config;
mod fhir;

use crate::config::{Kafka, Ssl};
use crate::fhir::mapper::FhirMapper;
// use crate::fhir::Mapper;
use config::AppConfig;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{debug, error, info};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::error::Error;
use std::sync::Arc;

async fn run(config: Kafka, mapper: FhirMapper) {
    // create consumer
    let consumer: StreamConsumer = create_consumer(config.clone());
    match consumer.subscribe(&[&config.input_topic]) {
        Ok(_) => {
            info!(
                "Successfully subscribed to topic: {:?}",
                &config.input_topic
            );
        }
        Err(error) => error!("Failed to subscribe to specified topic: {}", error),
    }
    let consumer = Arc::new(consumer);
    let producer = Arc::new(create_producer(config.clone()));

    let stream = consumer
        .stream()
        .map_err(|e| Box::new(e) as Box<dyn Error>)
        .try_for_each(|m| {
            let consumer = consumer.clone();
            let producer = producer.clone();
            let output_topic = config.output_topic.clone();
            let mapper = mapper.clone();

            {
                async move {
                    let (key, payload) = deserialize_message(&m);

                    info!(
                        "Message received: key: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                        key,
                        m.topic(),
                        m.partition(),
                        m.offset(),
                        m.timestamp()
                    );

                    if let Some(headers) = m.headers() {
                        for header in headers.iter() {
                            debug!("Header {:#?}: {:?}", header.key, header.value);
                        }
                    }

                    // filter tombstone records
                    if payload.is_none() {
                        return Ok(());
                    }


                    let Some(result) = (match mapper.map(payload.unwrap()) {
                        Ok(ok) => { ok }
                        Err(err) => {
                            panic!("Failed to map payload with [key={key}]: {}", err)
                        }
                    }) else {
                        commit_offset(&*consumer, &m);
                        return Ok(());
                    };

                    // send to output topic
                    let mut record = FutureRecord::to(&output_topic)
                        .key(&key)
                        .payload(result.as_str());
                    record.timestamp = m.timestamp().to_millis();

                    let produce_future = producer.send(record, Timeout::Never);
                    match produce_future.await {
                        Ok(delivery) => {
                            debug!("Message sent: key: {key}, partition: {}, offset: {}", delivery.partition,delivery.offset);
                            // store offset
                            commit_offset(&*consumer, &m);
                        }
                        Err((e, _)) => println!("Error: {:?}", e),
                    }

                    Ok(())
                }
            }
        });

    info!("Starting consumer");
    let error = stream.await;
    info!("Consumers terminated: {:?}", error);
}

fn commit_offset(consumer: &StreamConsumer, message: &BorrowedMessage) {
    consumer
        .store_offset_from_message(&message)
        .expect("Failed to store offset for message");
}

#[tokio::main]
async fn main() {
    let config = match AppConfig::new() {
        Ok(s) => s,
        Err(e) => panic!("Failed to parse app settings: {e:?}"),
    };
    let env = env_logger::Env::default().filter_or("RUST_LOG", config.app.log_level.clone());
    env_logger::init_from_env(env);

    // mapper
    let mapper = FhirMapper::new(config.clone()).expect("failed to create mapper");

    // run
    let num_partitions = 3;
    (0..num_partitions)
        .map(|_| tokio::spawn(run(config.kafka.clone(), mapper.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}

fn create_consumer(config: Kafka) -> StreamConsumer {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", config.brokers)
        .set("security.protocol", config.security_protocol)
        .set("enable.partition.eof", "false")
        .set("group.id", config.consumer_group)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", config.offset_reset)
        .set_log_level(RDKafkaLogLevel::Debug);

    set_ssl_config(c, config.ssl)
        .create()
        .expect("Failed to create Kafka consumer")
}

fn create_producer(config: Kafka) -> FutureProducer {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", config.brokers)
        .set("security.protocol", config.security_protocol)
        .set("compression.type", "gzip")
        .set("message.max.bytes", "6242880")
        .set_log_level(RDKafkaLogLevel::Debug);

    set_ssl_config(c, config.ssl)
        .create()
        .expect("Failed to create Kafka producer")
}

fn set_ssl_config(mut c: ClientConfig, ssl_config: Option<Ssl>) -> ClientConfig {
    if let Some(ssl) = ssl_config {
        if let Some(value) = ssl.ca_location {
            c.set("ssl.ca.location", value);
        }
        if let Some(value) = ssl.key_location {
            c.set("ssl.key.location", value);
        }
        if let Some(value) = ssl.certificate_location {
            c.set("ssl.certificate.location", value);
        }
        if let Some(value) = ssl.key_password {
            c.set("ssl.key.password", value);
        }
    }
    c
}

fn deserialize_message(m: &BorrowedMessage) -> (String, Option<String>) {
    let key = match m.key_view::<str>() {
        None => "",
        Some(Ok(k)) => k,
        Some(Err(e)) => {
            error!("Error while deserializing message key: {:?}", e);
            ""
        }
    };
    let payload = match m.payload_view::<str>() {
        None => None,
        Some(Ok(s)) => Some(s),
        Some(Err(e)) => {
            error!("Error while deserializing message payload: {:?}", e);
            None
        }
    };

    (key.to_owned(), payload.map(str::to_string).to_owned())
}

#[cfg(test)]
mod tests {
    use crate::config::AppConfig;
    use crate::fhir::mapper::FhirMapper;
    use crate::{deserialize_message, run};
    use fhir_model::r4b::resources::{Bundle, ResourceType};
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use serde_json::Value;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn test_run() {
        let _r = env_logger::try_init();
        const INPUT_TOPIC: &str = "input_topic";
        const OUTPUT_TOPIC: &str = "output_topic";

        // create mock cluster
        let mock_cluster = MockCluster::new(1).unwrap();
        mock_cluster
            .create_topic(INPUT_TOPIC, 1, 1)
            .expect("Failed to create input topic");
        mock_cluster
            .create_topic(OUTPUT_TOPIC, 1, 1)
            .expect("Failed to create output topic");

        let test_producer: FutureProducer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .create()
            .expect("Producer creation failed");

        let output_consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .set("group.id", "test-consumer")
            .create()
            .expect("Consumer creation failed");
        output_consumer.subscribe(&[OUTPUT_TOPIC]).unwrap();

        // input data
        let hl7_str = r#"MSH|^~\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20110613083617||ADT^A04|934576120110613083617|P|2.3||||\r
EVN|A04|20110613083617|||\r
PID|1|123456|123456||MOUSE^MICKEY^||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^^^theMainMouse@disney.com|||||1719|99999999||||||||||||||||||||\r
PV1|1|O|||||7^Disney^Walt^^MD^^^^|||||||||||||||||||||||||||||||||||||||||||||"#;

        let _res = send_record(test_producer.clone(), INPUT_TOPIC, hl7_str)
            .await
            .unwrap();

        // setup config
        let mut config = AppConfig::default();
        config.kafka.brokers = mock_cluster.bootstrap_servers();
        config.kafka.offset_reset = String::from("earliest");
        config.kafka.security_protocol = String::from("plaintext");
        config.kafka.consumer_group = String::from("test");
        config.kafka.input_topic = INPUT_TOPIC.to_owned();
        config.kafka.output_topic = OUTPUT_TOPIC.to_owned();

        // mapper
        let mapper = FhirMapper::new(config.clone()).expect("failed to create mapper");

        // run processor
        tokio::spawn(async move {
            run(config.kafka, mapper).await;
        });

        // get message from output topic
        let m = output_consumer.recv().await.unwrap();
        let (_, payload) = deserialize_message(&m);
        let raw: Value =
            serde_json::from_str(&*payload.expect("failed to read output message")).unwrap();
        let b: Bundle = serde_json::from_value(raw).unwrap();

        // assert resources
        assert_eq!(b.entry.len(), 1);
        assert!(
            b.entry
                .iter()
                .map(|e| e.clone().unwrap().resource.unwrap().resource_type())
                .all(|t| t == ResourceType::Patient)
        );
    }

    async fn send_record(
        producer: FutureProducer,
        topic: &str,
        payload: &str,
    ) -> OwnedDeliveryResult {
        producer
            .send_result(
                FutureRecord::to(topic)
                    .key("test")
                    .payload(payload)
                    .timestamp(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            .try_into()
                            .unwrap(),
                    ),
            )
            .unwrap()
            .await
            .unwrap()
    }
}
