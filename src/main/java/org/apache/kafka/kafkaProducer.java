package org.apache.kafka;

public class kafkaProducer {
	
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props);
		String topic = "myTopic";
		int partition = 0;
		String key = "testKey";
		String value = "testValue";
		prod.send(new ProducerRecord<String, String>(topic,partition,key, value));
		prod.close();
		}
}
