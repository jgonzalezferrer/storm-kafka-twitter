package org.apache.storm.storm_core;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class Top3App 
{
    public static final String LANG ="langs";

	
    public static void main( String[] args )
    {
    	
    	String[] langs = new String[]{"es","en"}; //take from input.
    	
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("currencySpout", new HashtagSpout());
        builder.setBolt("converterBolt", new ConditionalWindowBolt(),langs.length)
        .fieldsGrouping("currencySpout", HashtagSpout.NORMALSTREAM, new Fields(LANG));
        
        Config conf = new Config();
        //conf.setNumWorkers(langs.length); //Number of working nodes
        //conf.setDebug(true); //To debug. Remove when deployment
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("converterTopology",conf , builder.createTopology());

        Utils.sleep(10000);
              		       

        cluster.killTopology("converterTopology");

        cluster.shutdown();
    }
}
