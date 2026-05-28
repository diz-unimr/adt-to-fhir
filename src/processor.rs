use crate::ClientConfig;
use crate::config::{Kafka, Ssl};
use crate::error::{MappingError, ProcessingError};
use crate::fhir::mapper::FhirMapper;
use futures::TryStreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use log::{debug, error, info, trace, warn};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, TopicPartitionList};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

pub(crate) struct Processor {
    config: Kafka,
    mapper: Arc<FhirMapper>,
    producer: Arc<FutureProducer>,
    ctx: Context,
}

#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) on_commit: Option<Sender<TopicPartitionList>>,
    pub(crate) cancel: CancellationToken,
}
type ProcessingConsumer = StreamConsumer<Context>;
impl ClientContext for Context {}
impl ConsumerContext for Context {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        debug!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        debug!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        debug!("Committed offsets: {:?}", _offsets);

        if let Some(hook) = &self.on_commit {
            match result {
                Ok(_) => {
                    let sender = hook.clone();
                    let offsets = _offsets.clone();
                    tokio::spawn(async move {
                        if let Err(e) = sender.send(offsets).await {
                            error!("Failed to send commit_callback result: {e}");
                        }
                    });
                }
                Err(e) => {
                    warn!("Offset commit returned error: {e}");
                }
            }
        }
    }
}

impl Processor {
    pub(crate) fn new(config: Kafka, mapper: Arc<FhirMapper>, ctx: Context) -> Self {
        let producer = Arc::new(create_producer(config.clone()));
        Self {
            config,
            mapper,
            producer,
            ctx,
        }
    }

    pub(crate) async fn start(self) {
        let this = Arc::new(self);

        let tasks = (1..=this.config.num_partitions)
            .map(|id| {
                let this = this.clone();
                tokio::spawn(this.run(id))
            })
            .collect::<FuturesUnordered<_>>();

        join_all(tasks).await;
    }

    async fn run(self: Arc<Self>, id: i32) {
        loop {
            // create consumer
            let instance_id = format!("{}_{id}", self.config.consumer_group);
            let consumer = self.create_consumer(&instance_id);
            let topic = &self.config.input_topic;
            match consumer.subscribe(&[topic]) {
                Ok(()) => {
                    info!(
                        "Consumer[{id}] Successfully subscribed to topic {topic} with instance id: {instance_id}"
                    );
                }
                Err(e) => {
                    error!("Consumer[{id}] Failed to subscribe to topic {topic}: {e}");
                    // exit
                    break;
                }
            }

            let consumer = Arc::new(consumer);

            let stream = consumer
                .stream()
                .map_err(ProcessingError::from)
                .try_for_each(|m| self.process_message(m, id, consumer.clone()));

            info!(
                "Starting Consumer[{instance_id}] for topic {}",
                self.config.input_topic
            );
            match stream.await {
                // exit
                Err(ProcessingError::Cancelled(e)) => {
                    consumer.unsubscribe();
                    error!("{e}. Exiting.");
                    // exit loop
                    break;
                }
                Err(ProcessingError::Mapping(e)) => {
                    consumer.unsubscribe();
                    error!("{e}. Exiting.");
                    // cancel all consumer instances
                    self.ctx.cancel.cancel();
                    // exit loop
                    break;
                }
                // continue
                Err(ProcessingError::Kafka(e)) => {
                    consumer.unsubscribe();
                    error!("Failed to process message: {e}. Retrying..");
                }
                // exit
                Ok(()) => {
                    warn!("Consumer stream for topic {id} unexpectedly ended");
                    break;
                }
            };

            info!("Restarting consumer for topic {id} in 10 seconds...");
            if self.should_continue(Duration::from_secs(10)).await {
                // The token was cancelled
                info!("Consumer[{id}] for topic {topic} was stopped by cancellation");
                break;
            }
        }
    }

    async fn process_message(
        &self,
        m: BorrowedMessage<'_>,
        id: i32,
        consumer: Arc<ProcessingConsumer>,
    ) -> Result<(), ProcessingError> {
        let topic = m.topic();
        if self.ctx.cancel.is_cancelled() {
            return Err(ProcessingError::Cancelled(format!(
                "Consumer[{id}] for topic {topic} cancelled"
            )));
        }

        let (key, payload) = deserialize_message(&m);

        debug!("[Received] message from {topic}, key: {key}");
        trace!(
            "Message key: '{}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
            key,
            payload.as_deref().unwrap_or("[null]"),
            m.topic(),
            m.partition(),
            m.offset(),
            m.timestamp()
        );

        if let Some(headers) = m.headers() {
            for header in headers.iter() {
                trace!(
                    "Header {}:{}",
                    header.key,
                    header
                        .value
                        .map(String::from_utf8_lossy)
                        .unwrap_or_default()
                );
            }
        }

        // filter tombstone records
        if let Some(payload) = payload {
            let result = match self.mapper.map(&payload) {
                Ok(Some(r)) => r,
                Ok(None) => {
                    consumer.store_offset_from_message(&m)?;
                    return Ok(());
                }
                // handle error
                Err(e) => {
                    error!("Failed to map payload with [key={key}]: {e}");

                    return match e {
                        // TODO error metrics
                        MappingError::ResourceMappingError {
                            resource: _,
                            value: _,
                        } => {
                            error!("Fatal error, stopping Consumer[{id}].");
                            Err(ProcessingError::Mapping(e))
                        }
                        _ => {
                            consumer.store_offset_from_message(&m)?;
                            Ok(())
                        }
                    };
                }
            };

            // send to output topic
            let mut record = FutureRecord::to(&self.config.output_topic)
                .key(&key)
                .payload(result.as_str());
            record.timestamp = m.timestamp().to_millis();

            let produce_future = self.producer.send(record, Timeout::Never);
            match produce_future.await {
                Ok(delivery) => {
                    debug!(
                        "[Sent] key: {key}, partition: {}, offset: {}",
                        delivery.partition, delivery.offset
                    );
                    // store offset
                    consumer.store_offset_from_message(&m)?;
                }
                Err((e, _)) => error!("Error producing record: {:?}", e),
            }
        }

        match self.ctx.cancel.is_cancelled() {
            true => Err(ProcessingError::Cancelled(format!(
                "Consumer[{id}] for topic {topic} cancelled"
            ))),
            false => Ok(()),
        }
    }

    async fn should_continue(&self, wait: Duration) -> bool {
        select! {
            _ =  self.ctx.cancel.cancelled() => {
                true
            }
            _ = tokio::time::sleep(wait) => {
                false
            }
        }
    }

    fn create_consumer(&self, instance_id: &str) -> ProcessingConsumer {
        let config = self.config.clone();
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", config.brokers)
            .set("security.protocol", config.security_protocol)
            .set("enable.partition.eof", "false")
            .set("group.id", config.consumer_group)
            .set("group.instance.id", instance_id)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", config.offset_reset)
            .set_log_level(RDKafkaLogLevel::Debug);

        if let Some(ssl) = config.ssl {
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

        c.create_with_context(self.ctx.clone())
            .expect("Failed to create Kafka consumer")
    }
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

#[cfg(test)]
mod tests {
    use crate::config::{AppConfig, Kafka};
    use crate::fhir::mapper::FhirMapper;
    use crate::processor::{Context, Processor, deserialize_message};
    use crate::test_utils::tests::get_dummy_resources;
    use crate::tests::read_test_resource;
    use fhir_model::r4b::resources::{Bundle, ResourceType};
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{DefaultProducerContext, FutureProducer, FutureRecord};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_run() {
        let _r = env_logger::try_init();
        const INPUT_TOPIC: &str = "input_topic";
        const OUTPUT_TOPIC: &str = "output_topic";

        // create mock cluster
        let mock_cluster = setup_kafka(vec![("test", "test")]).await;
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
        let hl7_str = read_test_resource("a01_test.hl7");

        let _res = send_record(test_producer.clone(), INPUT_TOPIC, hl7_str.as_str())
            .await
            .unwrap();

        // setup config
        let config = AppConfig {
            kafka: Kafka {
                brokers: mock_cluster.bootstrap_servers(),
                offset_reset: String::from("earliest"),
                security_protocol: String::from("plaintext"),
                consumer_group: String::from("test"),
                input_topic: INPUT_TOPIC.to_owned(),
                output_topic: OUTPUT_TOPIC.to_owned(),
                num_partitions: 1,
                ssl: None,
            },
            app: Default::default(),
            fhir: Default::default(),
        };
        // mapper
        let mapper = Arc::new(FhirMapper {
            config: config.fhir,
            resources: get_dummy_resources(),
        });

        // processor
        let token = CancellationToken::new();
        let p = Processor::new(
            config.kafka,
            mapper,
            Context {
                cancel: token,
                on_commit: None,
            },
        );

        // run
        tokio::spawn(async move { p.start().await });

        // get message from output topic
        let m = output_consumer.recv().await;
        let (_, payload) = deserialize_message(&m.unwrap());
        let raw: Value =
            serde_json::from_str(&payload.expect("failed to read output message")).unwrap();
        let b: Bundle = serde_json::from_value(raw).unwrap();

        // assert resources
        assert_eq!(b.entry.len(), 8);
        assert!(
            b.entry
                .iter()
                .map(|e| e.clone().unwrap().resource.unwrap().resource_type())
                .all(|t| t == ResourceType::Patient
                    || t == ResourceType::Encounter
                    || t == ResourceType::Location
                    || t == ResourceType::Observation
                    || t == ResourceType::Organization)
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

    async fn setup_kafka<'a>(
        records: Vec<(&str, &str)>,
    ) -> MockCluster<'a, DefaultProducerContext> {
        // create mock cluster
        let mock_cluster = MockCluster::new(3).unwrap();
        let mock_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .create()
            .expect("Producer creation error");

        for record in records {
            let _ = mock_cluster.create_topic(record.0, 3, 3);

            send_record(mock_producer.clone(), record.0, record.1)
                .await
                .unwrap();
        }

        mock_cluster
    }
}
