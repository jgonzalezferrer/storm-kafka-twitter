package org.apache.storm.storm_core;

import org.apache.storm.storm_core.DefaultTreeMap;
import org.apache.storm.storm_core.DefaultHashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConditionalWindowBolt extends BaseRichBolt {

	// Map to save the list of hashtags occurrences between conditional windows for each language.
	public Map<String, Map<String, Integer>> langMap = new HashMap<String, Map<String, Integer>>();

	// Key words for conditional windows. The words that initialize conditional windows.
	public List<String> keyWords = new ArrayList<String>();

	// Map to store lapses between conditional windows. We will store words in the hashtags occurrences if this flag is on.	
	public HashMap<String, Boolean> conditionalWindow = new DefaultHashMap<String, Boolean>(false);

	private String[] langs = new String[]{"es","en"}; //take from input.
	
	private int counter = 0;
	
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

		// TreeMap to save the words alphabetically.
		for(int i=0; i<langs.length; i++){
			langMap.put(langs[i], new DefaultTreeMap<String, Integer>(0));
		}
		
		//Adding keywords: take from input
		keyWords.add("es:jaula");
		keyWords.add("en:brexit");
	}
	
	private String top3Algorithm(String langField){		
		int[] top3 = new int[]{0, 0, 0};
		String[] top3k = new String[]{"null", "null", "null"};
		for (Map.Entry<String, Integer> entry : langMap.get(langField).entrySet()) {
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
		String valueField = (String) tuple.getValueByField(HashtagSpout.HASHTAG);
		String langField =  (String) tuple.getValueByField(HashtagSpout.LANG);	

		if (keyWords.contains(langField+":"+valueField)) {
			// Activate or deactivate.
			boolean state = conditionalWindow.get(langField);
			conditionalWindow.put(langField, !state);

			// From true to negative -> send conditional windows
			if(state){ // It is now false, finishing windows
				
				// Calculate top3 from such conditional windows. 
				String msg = top3Algorithm(langField);
				System.out.println(msg);

				langMap.put(langField, new DefaultHashMap<String, Integer>(0)); //Reset window accumulator

			}
		}

		// If already activated
		else if(conditionalWindow.get(langField)){
			// Update occurrence
			Map<String, Integer> aux = langMap.get(langField);
			aux.put(valueField, aux.get(valueField)+1);
		}
	}


	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

}
