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
    public static final String CURRENCYFIELDNAME ="currencyID";
    public static final String CURRENCYFIELDVALUE ="value";
    public static final String CURRENCYOUSTREAM ="currencyStream";

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }
    
    private static String[] testArray = new String[]{"lebron","lebron","lebron","lebron","durant",
    		"durant","durant","westbrook",
    		"ibaka","rubio"};

    private Values randomValue(){
        int value = (int) Math.floor(Math.random()*10);
        return new Values(testArray[value]);
    }

    public void nextTuple() {
        Values randomValue = this.randomValue();
        System.out.println("emitting " + randomValue);
        collector.emit(CURRENCYOUSTREAM, randomValue);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(CURRENCYOUSTREAM, new Fields(CURRENCYFIELDVALUE));
    }
}
