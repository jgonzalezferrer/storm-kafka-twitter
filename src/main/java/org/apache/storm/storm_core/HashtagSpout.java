package org.apache.storm.storm_core;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class HashtagSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    public static final String LANG ="langs";
    public static final String HASHTAG = "value";
    public static final String CURRENCYOUSTREAM ="currencyStream";

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }
    
    private int cont=0;
    
//    private static String[] testArray = new String[]{"lebron","lebron","lebron","lebron","durant",
//    		"durant","durant", "ibaka","rubio","curry", "rubio"};
//    private static String[] testArray = new String[]{"es,casa", "es,ordenador", "es,ordenador", "es,ordenador",
//    		"es,casa", "es,coche", "es,coche", "es,jaula", "en,house", "en,house", "en,house", 
//    		"en,tree","en,tree", "en,toilet", "en,toilet", "en,jail"};
    private static String[] testArray = new String[]{"es,casa","en,ashtray", "es,pepe","es,pepe","en,brexit", "es,ordenador", "en,trump","es,jaula", "en,hillary","es,ordenador", "es,ordenador",
    		"es,coche","en,hillary", "es,coche", "en,yesorno","es,pepe", "en,brexit","es,jaula", "es,casa","en,ashtray"};
    
    private Values randomValue(){
        //int rand = (int) Math.floor(Math.random()*testArray.length);
    	String[] parts = testArray[cont++%testArray.length].split(",");
    	String lang = parts[0];
    	String value = parts[1];
        return new Values(lang, value);
    }

    public void nextTuple() {
        Values randomValue = this.randomValue();
        //System.out.println("emitting " + randomValue);
        collector.emit(CURRENCYOUSTREAM, randomValue);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(CURRENCYOUSTREAM, new Fields(LANG, HASHTAG));
    }
}
