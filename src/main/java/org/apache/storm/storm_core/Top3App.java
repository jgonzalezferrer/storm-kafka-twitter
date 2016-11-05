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
        
       
        

        cluster.killTopology("converterTopology");

        cluster.shutdown();
    }
}
