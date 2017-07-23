package ly;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface ICTPFlowTransform<K, V> {
	public List<ProducerRecord<K, V>> transform(ConsumerRecords<K, V> consumerRecords, String producerTopic);
}