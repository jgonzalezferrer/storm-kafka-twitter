package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import master2016.DefaultHashMap;
import master2016.DefaultTreeMap;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConditionalWindowBolt extends BaseRichBolt {

	// Map to save the list of hashtags occurrences between conditional windows for each language.
	public Map<String, Integer> langMap = new DefaultTreeMap<String, Integer>(0);

	// Key words for conditional windows. The words that initialize conditional windows.
	public String keyWord = "";

	// Map to store lapses between conditional windows. We will store words in the hashtags occurrences if this flag is on.	
	
	public boolean conditionalWindowAct = false;
	
	private int counter = 0;
	
	//Constructor executed in nimbus (central node)
	public ConditionalWindowBolt(){
		System.out.println("Constructorrrrrrrrr");
	}
	
	
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		// Executed in the workers
		// TreeMap to save the words alphabetically.
		
		//Adding keywords: take from inpu
		System.out.println("Prepareeeeeeeeeeeeeeeeeee");
	}
	
	private String top3Algorithm(String langField){
		
		int[] top3 = new int[]{0, 0, 0};
		String[] top3k = new String[]{"null", "null", "null"};
		for (Map.Entry<String, Integer> entry : langMap.entrySet()) {
			int value = entry.getValue();				
			String key = entry.getKey();					

			if(value > top3[0]){					
				top3[2] = top3[1];
				top3k[2] = top3k[1];
				top3[1] = top3[0];
				top3k[1] = top3k[0];
				top3[0] = value;
				top3k[0] = key;
			}
			else if(value > top3[1]){
				top3[2] = top3[1];
				top3k[2] = top3k[1];
				top3[1] = value;
				top3k[1] = key;
			}
			else if(value > top3[2]){
				top3[2] = value;
				top3k[2] = key;
			}
		}
		
		String toReturn = counter+++","+langField+",";
		for(int i=0; i<top3.length; i++){ //Print top 3 keys + its values
			if(i==top3.length-1){
				toReturn += top3k[i]+","+top3[i];
			}
			else{
				toReturn += top3k[i]+","+top3[i]+",";
				
			}
		}
		return toReturn;		
	}
	

	public void execute(Tuple tuple) {
	
		String valueField = (String) tuple.getValueByField(TwitterScheme.KafkaValue);
		
		
		System.out.println("Hashtag received from Kafka: "+valueField);
		/*
		
		String valueField = (String) tuple.getValueByField(HashtagSpout.HASHTAG);
		String langField =  (String) tuple.getValueByField(HashtagSpout.LANG);	
		
		//Take keyword from file
		
		if(langField.equals("es"))
			keyWord = langField+":jaula";
		else if(langField.equals("en"))
			keyWord = langField+":brexit";
				
		if (keyWord.equals(langField+":"+valueField)) {
			// Activate or deactivate.
			boolean state = conditionalWindowAct;
			conditionalWindowAct=!state;
			// From true to negative -> send conditional windows
			if(state){ // It is now false, finishing windows
				// Calculate top3 from such conditional windows.
				String msg = top3Algorithm(langField);
				System.out.println(msg);

				langMap = new DefaultTreeMap<String, Integer>(0); //Reset window accumulator
				//langMap.clear();
			}
		}

		// If already activated
		else if(conditionalWindowAct){
			// Update occurrence
			langMap.put(valueField, langMap.get(valueField)+1);
		}*/
	}
	
		//To parallelize: Send each language from the spout to each bolt for a language-specific bolt

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

}
