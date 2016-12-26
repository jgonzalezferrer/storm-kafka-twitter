package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;

/** ConditionalWindowBolt: bolt for retrieving tweets between conditional windows.
 * 
 * Each of the spouts created given a language are connected to this first bolt. When the keyword appears, the bolt starts gathering the
 * tweets from this conditional window, sending each of them to the next bolt, until the keyword appears again. 
 * 
 * 
 * @param: keyWord, token for the given language.
 *
 */
public class ConditionalWindowBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5379209227989927713L;
	
	/*
	 * String defining the hashtag field.
	 */
	public static final String HASHTAGFIELD = "hashtagValue";
	
	/*
	 * String defining the stop condition field.
	 */
	public static final String STOPCONDITIONFIELD = "stopField";
	
	/*
	 * Key word for the conditional window. The word that initialize and stop conditional windows.
	 */
	private String keyWord;
	
	/*
	 *  We will send words in between the hashtags occurrences if this flag is on.
	 */	
	private boolean conditionalWindowAct = false;
	
	/*
	 * Output collector to emit tuples to the next bolt.
	 */
	private OutputCollector col;	
	
	/*
	 * Constructor executed in nimbus (central node).
	 */
	public ConditionalWindowBolt(String keyWord){
		this.keyWord = keyWord;		
	}
	
	/*
	 * Method executed in the workers that provides the bolt with an output collector used to emit tuples from this bolt.
	 *
	 * @param: conf, the Storm configuration for this spout.
	 * @param: context, information about the task within the topology.
	 * @param: collector, used to emit tuples from this spout.
	 */
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.col = collector;
	}

	/*
	 * Process a single tuple of input.
	 * 
	 * @param tuple, tuple to process.
	 */
	public void execute(Tuple tuple) {
		// Read the hashtag value from the spout.
		String valueField = (String) tuple.getValueByField(KafkaTwitterSpout.KAFKAFIELD);		
		
		// If the hashtag is the input, we start/finish the conditional window.
		if (this.keyWord.equals(valueField)) {	
			
			// If the conditional windows was activated, we emit a message to finish.
			if(this.conditionalWindowAct){
				this.col.emit(tuple, new Values("0", "1"));
			}
			// Change the conditional value to the opposite state (from start to stop, or from stop to start).
			this.conditionalWindowAct = !this.conditionalWindowAct;
		}
		
		// If the  hashtag is not the keyword and the conditional windows is activated, we emit the tuple.
		else if(conditionalWindowAct){ 
			// Send the occurrence to the next Bolt.
			this.col.emit(tuple, new Values(valueField, "0"));
		}
	}
	
	/*
	 * This method declares the outputs field for the component. In this case, two String fields with a) the hashtag and b) the stop condition.
	 */
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(HASHTAGFIELD, STOPCONDITIONFIELD));
	}

}
