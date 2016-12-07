package upm.cloud.project1.twitterApp;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TwitterKafkaProducer {
	private String kafkaUrl;
	private String topic;
	private static Properties props = new Properties();
	private static KafkaProducer<String, byte[]> prod = new KafkaProducer<String, byte[]>(props);
	public TwitterKafkaProducer(String kafkaUrl, String topic){
		this.kafkaUrl=kafkaUrl;
		this.topic = topic;
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");		 
	}
	

	public void sendTweet(String tweet){
		//TODO: partition
		prod.send(new ProducerRecord<String, byte[]>(topic,tweet.getBytes()));
	}

	public void close(){
		prod.close();
		}

}
