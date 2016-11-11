package org.apache.storm.storm_core;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Top3App 
{
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("currencySpout", new HashtagSpout());
        builder.setBolt("converterBolt", new ConditionalWindowBolt()).localOrShuffleGrouping("currencySpout", HashtagSpout.CURRENCYOUSTREAM);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("converterTopology", new Config(), builder.createTopology());

        Utils.sleep(10000);
        
        
       //Send separate languages to different bolts, each bolt process a language and the result is received 
        //for each language, create a bolt and send each instance of that language to that bolt
	//Instead of shuffle, .fieldsGrouping("tempSpout", TemperatureSpout.TEMPERATURE_STREAMNAME, new Fields("roomID"))
      //config.setNumWorkers(4)
      //config.setDebug(true) to debug
      		
      //Remove setDebug and every printout before deploying because it slows down the app
       
        

        cluster.killTopology("converterTopology");

        cluster.shutdown();
    }
}
