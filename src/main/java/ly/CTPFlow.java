package ly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class CTPFlow<K, V> {

	private Properties consumerProps;
	private Properties producerProps;
	private KafkaConsumer<K, V> consumer;
	private KafkaProducer<K, V> producer;
	private long pollTimeout;
	private ICTPFlowTransform<K, V> transform;

	private String producerTopic;

	private String consumerTopic;
	private int partition;

	public CTPFlow() {
		this.pollTimeout = 1000L; // 默认poll过期时间
	}

	public CTPFlow<K, V> setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
		return this;
	}

	public CTPFlow<K, V> constructKafkaConsumer(Properties consumerProps) {
		this.consumerProps = consumerProps;
		this.consumer = new KafkaConsumer<K, V>(consumerProps);
		return this;
	}

	public CTPFlow<K, V> constructKafkaProducer(Properties producerProps) {
		this.producerProps = producerProps;
		this.producer = new KafkaProducer<K, V>(producerProps);
		return this;
	}

	public CTPFlow<K, V> setCTPFlowTransform(ICTPFlowTransform<K, V> transform) {
		this.transform = transform;
		return this;
	}

	public CTPFlow<K, V> setProducerTopic(String producerTopic) {
		this.producerTopic = producerTopic;
		return this;
	}

	public CTPFlow<K, V> setConsumerTopic(String consumerTopic, int partition) {
		this.consumerTopic = consumerTopic;
		this.partition = partition;
		return this;
	}

	private Map<TopicPartition, OffsetAndMetadata> getUncommittedOffsets() {
		Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
		for (TopicPartition topicPartition : consumer.assignment()) {
			offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)));
		}
		return offsetsToCommit;
	}

	private void resetToLastCommittedPositions() {
		for (TopicPartition topicPartition : consumer.assignment()) {
			OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
			if (offsetAndMetadata != null) {
				consumer.seek(topicPartition, offsetAndMetadata.offset());
			} else {
				consumer.seekToBeginning(Collections.singleton(topicPartition));
			}
		}
	}

	public void execute() {
		if (consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG) == null) {
			throw new IllegalArgumentException("consumer.group.id must not be null.");
		}
		if (consumerTopic == null || "".equals(consumerTopic)) {
			throw new IllegalArgumentException("consumerTopic must not be null.");
		}
		if (producerTopic == null || "".equals(producerTopic)) {
			throw new IllegalArgumentException("producerTopic must not be null.");
		}
		if (producer == null) {
			throw new IllegalArgumentException("producer must not be null.");
		}
		if (consumer == null) {
			throw new IllegalArgumentException("consumer must not be null.");
		}
		if (transform == null) {
			throw new IllegalArgumentException("transform must not be null.");
		}

		TopicPartition assignPartition = new TopicPartition(consumerTopic, partition);
		consumer.assign(Collections.singleton(assignPartition));
		producer.initTransactions();
		resetToLastCommittedPositions();
		while (true) {
			ConsumerRecords<K, V> consumerRecords = consumer.poll(pollTimeout);
			if (!consumerRecords.isEmpty()) {
				try {
					producer.beginTransaction();
					List<ProducerRecord<K, V>> producerRecords = transform.transform(consumerRecords, producerTopic);
					if (producerRecords != null) {
						for (ProducerRecord<K, V> producerRecord : producerRecords) {
							producer.send(producerRecord);
						}
					}
					producer.sendOffsetsToTransaction(getUncommittedOffsets(),
							consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
					producer.commitTransaction();
				} catch (ProducerFencedException e1) {
					throw e1;
				} catch (OutOfOrderSequenceException e2) {
					throw e2;
				} catch (KafkaException e3) {
					producer.abortTransaction();
					resetToLastCommittedPositions();
				}
			}
		}
	}

	public static void main(String[] args) {
		Properties consumerProps = new Properties();
		Properties producerProps = new Properties();

		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumeTransformProduceApp5");
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.1.171.229:9092");
		consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.1.171.229:9092");
		producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "ConsumeTransformProduceApp5");

		new CTPFlow<String, String>().setPollTimeout(1000L).setConsumerTopic("ctp1", 0).setProducerTopic("ctp2")
				.constructKafkaConsumer(consumerProps).constructKafkaProducer(producerProps)
				.setCTPFlowTransform(new ICTPFlowTransform<String, String>() {
					@Override
					public List<ProducerRecord<String, String>> transform(
							ConsumerRecords<String, String> consumerRecords, String producerTopic) {
						List<ProducerRecord<String, String>> producerRecords = new ArrayList<ProducerRecord<String, String>>();
						for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
							producerRecords.add(new ProducerRecord<String, String>(producerTopic,
									consumerRecord.partition(), consumerRecord.key(),
									consumerRecord.value().toString() + consumerRecord.value().toString()));
						}
						return producerRecords;
					}
				}).execute();
	}

}