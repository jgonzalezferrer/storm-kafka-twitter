package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import master2016.DefaultHashMap;
import master2016.DefaultTreeMap;

import java.util.List;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConditionalWindowBolt extends BaseRichBolt {

	// Map to save the list of hashtags occurrences between conditional windows for each language.
	public Map<String, Integer> langMap = new DefaultTreeMap<String, Integer>(0);

	// Key words for conditional windows. The words that initialize conditional windows.
	private String keyWord;
	private String langField;
	private OutputCollector col;

	// Map to store lapses between conditional windows. We will store words in the hashtags occurrences if this flag is on.	
	
	public boolean conditionalWindowAct = false;
	
	private int counter = 0;
	
	PrintWriter writer;
	
	//Constructor executed in nimbus (central node)
	public ConditionalWindowBolt(String keyWord, String langField){
		this.keyWord = keyWord;
		this.langField = langField;
		
	}
	
	
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		// Executed in the workers
		// TreeMap to save the words alphabetically.
		col=outputCollector;
		//Adding keywords: take from input
	}

	

	public void execute(Tuple tuple) {
	
		String valueField = (String) tuple.getValueByField(TwitterScheme.KafkaValue);
		
		
		System.out.println("Hashtag received from Kafka in conditional window: "+valueField);
		
		
		//Take keyword from file
		
	
				
		if (keyWord.equals(valueField)) {
			// Activate or deactivate.
			boolean state = conditionalWindowAct;
			conditionalWindowAct=!state;
			// From true to negative -> send conditional windows
			if(state){ // It is now false, finishing windows
				col.emit(tuple, new Values("0", "1"));
			}
		}

		// If already activated
		else if(conditionalWindowAct){
			// Update occurrence
			col.emit(tuple, new Values(valueField, "0"));
		}
	}
	
		//To parallelize: Send each language from the spout to each bolt for a language-specific bolt

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("value", "endField"));
	}

}
