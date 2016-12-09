package master2016;


import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class Top3App 
{
    public static final String LANG ="langs";
    
    public static final String KAFKA_SPOUT = "kafkaSpout";
    public static final String LANGUAGE_BOLT = "languageBolt";
    
  
	
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
    	
    	HashMap<String, String> langKeywords = new HashMap<String, String>();    	
    	for(int i=0; i<langs.length; i++){
    		String [] langKey = langs[i].split(":");
    		langKeywords.put(langKey[0], langKey[1]);
    	}
    	
    
    
    	
    	
//    	String langList = "";
//    	String [] keyWordsList = {""};
//    	String [] langs = {""};
//    	String zookeeperUrl="localhost:2181";
//    	String topologyName="p";
//    	String folder = "p";
    	
        TopologyBuilder builder = new TopologyBuilder();
        
        // Creating spout and bolt for each language
    	for(Map.Entry<String, String> entry : langKeywords.entrySet()){
    		  builder.setSpout("SPOUT_"+entry.getKey(), 
    				  new KafkaTwitterSpout(zookeeperUrl,entry.getKey()).getKafkaSpout());
    	       
    		  builder.setBolt("BOLT_WINDOW"+entry.getKey(), new ConditionalWindowBolt(entry.getValue(), entry.getKey()))
    	        	.shuffleGrouping("SPOUT_"+entry.getKey());
    		      		  
    		  builder.setBolt("BOLT_COUNT"+entry.getKey(), new CountBolt(entry.getKey(),folder)).shuffleGrouping("BOLT_WINDOW"+entry.getKey());
    	}

//        
//        //.fieldsGrouping("currencySpout", HashtagSpout.NORMALSTREAM, new Fields(LANG));
//        
//        
//        //Possible further parallelization: Send from first language bolt towards a bolt that counts
//        //the words, switch from bolt to bolt for each tweet (modulus bolt) and starts counting as soon as possible
//        //so the counting is distributed. Then, unite those bolts in another final bolt per language 
//        
        Config conf = new Config();
        conf.setNumWorkers(3); //Number of working nodes
        //There's no great reason to use more than one worker per topology per machine.
//        //conf.setDebug(true); //To debug. Remove when deployment
//    
        try {
			StormSubmitter.submitTopology(topologyName,conf , builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology(topologyName,conf , builder.createTopology());

        //Utils.sleep(10000);
              		       

        //cluster.killTopology(topologyName);

        //cluster.shutdown();
    }
}
