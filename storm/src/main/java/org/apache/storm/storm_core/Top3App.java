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
    	if (args.length!=4){
    		System.err.println("Incorrect number of parameters, it must be 4");
    		return;
    	}
    	String langList = args[0];
    	String [] langs = langList.split(",");
    	String zookeeperUrl=args[1];
    	String topologyName=args[2];
    	String folder = args[3];
    	
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaTwitterSpout(zookeeperUrl,"en").getKafkaSpout());
        builder.setBolt("LanguageBolt", new ConditionalWindowBolt(),langs.length)
        	.shuffleGrouping("KafkaSpout");
        //.fieldsGrouping("currencySpout", HashtagSpout.NORMALSTREAM, new Fields(LANG));
        
        
        //Possible further parallelization: Send from first language bolt towards a bolt that counts
        //the words, switch from bolt to bolt for each tweet (modulus bolt) and starts counting as soon as possible
        //so the counting is distributed. Then, unite those bolts in another final bolt per language 
        
        Config conf = new Config();
        //conf.setNumWorkers(langs.length); //Number of working nodes
        //conf.setDebug(true); //To debug. Remove when deployment
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName,conf , builder.createTopology());

        Utils.sleep(10000);
              		       

        cluster.killTopology(topologyName);

        cluster.shutdown();
    }
}
