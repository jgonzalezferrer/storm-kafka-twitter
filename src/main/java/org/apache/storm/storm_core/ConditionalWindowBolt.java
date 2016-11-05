package org.apache.storm.storm_core;

import org.apache.storm.storm_core.DefaultHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ConditionalWindowBolt extends BaseRichBolt {
	
	// Map to store the list of hashtags occurrences between conditional windows.
	// Format: (Key, cont) -> default: 0.
	// TODO: hashmap for each lang? list of hashmaps and hash?
	public HashMap<String, Integer> map = new DefaultHashMap<String, Integer>(0);
	
	// Key words for conditional windows. The words that initialize conditional windows.
	// Format: lang:word.
	// TODO: initialize in the prepare method?
	public List<String> keyWords = new ArrayList<String>();
	
	// Map to store lapses between conditional windows. We will store words in the hashtags occurences if this flag is on.
	// Format: (lang, True/False) -> default: False.	
	public HashMap<String, Boolean> conditionalWindow = new DefaultHashMap<String, Boolean>(false);
	
	// DEBUG.
	private int cont = 0;
	
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    	
    }    

    public void execute(Tuple tuple) {
        String valueField = (String) tuple.getValueByField(HashtagSpout.CURRENCYFIELDVALUE);
        // TODO: replace, receive as a parameter from the Spout.
        String langField = "lang";        
        
        if (keyWords.contains(langField+":"+valueField)) {
        	//Activate or desactivate
        	boolean state = conditionalWindow.get(langField);
        	conditionalWindow.put(langField, state);
        	
        	// From true to negative -> send conditional windows
        	if(!state){
        		// Calculate top3 from such conditional windows.
        		// Reset array for that lang.        		
        	}
        }
             	
        // If already activated
        else if(conditionalWindow.get(langField)){
	        // Update occurrence
	        map.put(valueField, map.get(valueField) + 1);
	        
	        System.out.println("Reciving " + valueField);
        }
        
        // DEBUG
    	if(cont % 50 == 0){    		 
	        for (String key : map.keySet()) {
	            System.out.println(key + " " + map.get(key));
	        }
    	}
    
        
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    
}
