package master2016;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/** KafkaTwitterSpout: source of tweets received from Kafka.
 * 
 * @param: kafkaBroker, string IP:port of the Kafka Broker.
 * @param: topic, topic for each of the languages.
 * 
 */
public class KafkaTwitterSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1583176839390965103L;
	private String kafkaBroker;
	private String topic;
	
	/*
	 * Name of the language stream.
	 */
	public static final String LANGUAGESTREAM = "languageStream";
	
	/*
	 * String identifying the kakka value.
	 */
	public static final String KAFKAFIELD = "value";
	
	/*
	 * Collector for emiting tuples from the spout.
	 */
	private SpoutOutputCollector collector;
	
	/*
	 * Kafka Consumer to read streams of data from topics in the Kafka cluster.
	 */
	private KafkaConsumer<String, String> consumer;

	public KafkaTwitterSpout(String kafkaBroker, String topic){
		this.kafkaBroker = kafkaBroker;
		this.topic = topic;
	}
	
	/*
	 * This method is called when a task for this component is initialized within a worker in the cluster.
	 * 
	 * @param: conf, the Storm configuration for this spout.
	 * @param: context, information about the task within the topology.
	 * @param: collector, used to emit tuples from this spout.
	 */
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// Consumer configuration.
		this.collector = collector;
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaBroker);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "LANGS");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topic));
	}

	/*
	 * This method request to emit tuples to the output collector.
	 */
	public void nextTuple() {
		ConsumerRecords<String, String> records = consumer.poll(5);
		for (ConsumerRecord<String, String> record : records){
			Values val = new Values(record.value());
			collector.emit(LANGUAGESTREAM, val);
		}
	}
	
	/*
	 * This method declares the outputs field for the component. In this case, a String kafka field.
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(LANGUAGESTREAM, new Fields(KAFKAFIELD));
	}

}