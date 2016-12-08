package master2016;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TwitterKafkaProducer {
	private String kafkaUrl;
	private static Properties props = new Properties();
	private static KafkaProducer<String, String> prod;
	
	public TwitterKafkaProducer(String kafkaUrl){
		this.kafkaUrl=kafkaUrl;
		props.put("bootstrap.servers", kafkaUrl);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");		 
		
		prod = new KafkaProducer<String, String>(props);
		

	}
	

	public void sendTweet(Tweet tweet){
		prod.send(new ProducerRecord<String, String>(tweet.getLang(), tweet.getHashtag()));
	}

	public void close(){
		prod.close();
		}

}
