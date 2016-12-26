package master2016;


import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/** Top3App: finds the trending topics for a given set of languages and period.
 *  
 * Storm topology for calculating the three more common hashtags (top3) 
 * in a set of languages over a configurable period of time (conditional window).
 * 
 * @author Antonio Javier González Ferrer
 * @author Aitor Palacios Cuesta
 *
 */
public class Top3App {
	/*
	 * String with the list of languages we are interested in and the associated tokens.
	 * Example: en:house,es:casa,pl:universidade
	 */
	private static String langList;
	
	/*
	 * String IP:port of the Kafka Broker
	 */
	private static String kafkaBroker;
	
	/*
	 * String identifying the topology in the Storm Cluster.
	 */
	private static String topologyName;
	
	/*
	 * Path to the folder used to store the output files.
	 */
	private static String folder;


	public static void main(String[] args){

		// The number of arguments must be exatly 4.
		if (args.length!=4){
			System.err.println("Incorrect number of parameters, it must be 4");
			return;
		}

		langList = args[0];
		String [] langs = langList.split(",");
		kafkaBroker=args[1];
		topologyName=args[2];
		folder = args[3];

		// Create a map of keywords for each language.
		HashMap<String, String> langKeywords = new HashMap<String, String>();    	
		for(int i=0; i<langs.length; i++){
			String [] langKey = langs[i].split(":");
			langKeywords.put(langKey[0], langKey[1]);
		}   

		/* 
		 * The Topology is defined as follows:
		 * 
		 * 1) For each of the languages (topics received from Kafka), we create one spout and two bolts.
		 * 2) The spout reads from Kafka and sends the tuples to the first bolt.
		 * 3) We use one bolt to start and stop the conditional window based on the keyword and another one to count the 3 most frequent hashtags 
		 *    and write them to a file. 
		 * 
		 * We do such distribution in order to parallelize the workflow through the cluster. Between every window there can be many hashtags received, 
		 * which are not used and can be discarded by the first bolt while the second one keeps counting or writing to file.
		 * 
		 * Note: we consider that a conditional window is always defined by a start token and a stop token. Moreover, the stop token of a conditional window
		 * cannot be the start token of the next one.
		 */

		TopologyBuilder builder = new TopologyBuilder();
		for(Map.Entry<String, String> entry : langKeywords.entrySet()){
			builder.setSpout("SPOUT_"+entry.getKey(), 
					new KafkaTwitterSpout(kafkaBroker, entry.getKey()));

			builder.setBolt("BOLT_WINDOW"+entry.getKey(), new ConditionalWindowBolt(entry.getValue()))
			.shuffleGrouping("SPOUT_"+entry.getKey(), KafkaTwitterSpout.LANGUAGESTREAM);

			builder.setBolt("BOLT_COUNT"+entry.getKey(), new CountBolt(entry.getKey(),folder))
			.shuffleGrouping("BOLT_WINDOW"+entry.getKey());
		}

		Config conf = new Config();
		conf.setNumWorkers(3); //Number of working nodes defined to take advantage of the mini cluster configuration.

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

		// Local configuration of the topology.
		/*
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName,conf , builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology(topologyName);
		cluster.shutdown();
		*/
	}
}
