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
        builder.setBolt("converterBolt", new ConditionalWindowBolt("Lang bolt"),langs.length)
        .fieldsGrouping("currencySpout", HashtagSpout.NORMALSTREAM, new Fields(LANG));
       /*
        for (int i=0;i<langs.length;i++){
        	String str1 = "converterBolt "+langs[i];
        	String str2 = "currencySpout";
         builder.setBolt(str1, new ConditionalWindowBolt(langs[i]),2)
         .fieldsGrouping(str2, HashtagSpout.NORMALSTREAM, new Fields(LANG));
       //Instead of shuffle, which sends everything to the same spout
        }*/
        
        Config conf = new Config();
        //conf.setNumWorkers(langs.length); //Number of working nodes
        //conf.setDebug(true); //To debug. Remove when deployment
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("converterTopology",conf , builder.createTopology());

        Utils.sleep(10000);
        
        
       //Send separate languages to different bolts, each bolt process a language and the result is received 
        //for each language, create a bolt and send each instance of that language to that bolt
        
        //Key = language
        //Get w
              		       
        

        cluster.killTopology("converterTopology");

        cluster.shutdown();
    }
}
