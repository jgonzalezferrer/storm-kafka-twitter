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

	// Map to store the list of hashtags occurrences between conditional windows.
	// Format: (Key, cont) -> default: 0.
	// TODO: hashmap for each lang? list of hashmaps and hash?
	public Map<String, Map<String, Integer>> langMap = new HashMap<String, Map<String, Integer>>();


	// Key words for conditional windows. The words that initialize conditional windows.
	// Format: lang:word.
	// TODO: initialize in the prepare method?
	public List<String> keyWords = new ArrayList<String>();

	// Map to store lapses between conditional windows. We will store words in the hashtags occurences if this flag is on.
	// Format: (lang, True/False) -> default: False.	
	public HashMap<String, Boolean> conditionalWindow = new DefaultHashMap<String, Boolean>(false);

	// DEBUG.
	private int cont = 0;

	private String[] langs = new String[]{"es","en"}; //take from input.
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

		for(int i=0; i<langs.length; i++){
			langMap.put(langs[i], new DefaultTreeMap<String, Integer>(0));
		}
		//Adding keywords: take from input
		keyWords.add("es:jaula");
		keyWords.add("en:brexit");
		
		for(int i=0;i<keyWords.size(); i++){
			System.out.println(keyWords.get(i));
		}
	}

	public void execute(Tuple tuple) {
		String valueField = (String) tuple.getValueByField(HashtagSpout.HASHTAG);
		String langField =  (String) tuple.getValueByField(HashtagSpout.LANG);		


		if (keyWords.contains(langField+":"+valueField)) {
			//Activate or desactivate
			boolean state = conditionalWindow.get(langField);
			conditionalWindow.put(langField, !state);

			// From true to negative -> send conditional windows
			if(state){ // It is now false, finishing windows
				// Calculate top3 from such conditional windows. 
					int[] top3 = new int[]{0, 0, 0};
					String[] top3k = new String[]{"", "", ""};
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
					for(int i=0; i<top3.length; i++){ //Print top 3 keys + its values
						System.out.print(" key: "+top3k[i]+" value: "+top3[i]);
					}
					System.out.println();

				langMap.put(langField, new DefaultHashMap<String, Integer>(0)); //Reset window accumulator

			}
		}

		// If already activated

		else if(conditionalWindow.get(langField)){
			// Update occurrence
			Map<String, Integer> aux = langMap.get(langField);
			aux.put(valueField, aux.get(valueField)+1);
		}

		// DEBUG
		//		if(cont != 0 && cont % 11 == 0){  			
		//			
		//			
		//			System.out.println("new lang!!");
		//		}
	}




	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

}
