package org.apache.storm.storm_core;

import java.util.UUID;

import kafka.api.OffsetRequest;
/*
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;*/

import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;

public class KafkaTwitterSpout {
	
	private KafkaSpout kafkaSpout;
	public KafkaTwitterSpout(String zkConn, String topicName){
	BrokerHosts hosts = new ZkHosts(zkConn);
	SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
	spoutConfig.scheme =  new SchemeAsMultiScheme(new TwitterScheme());
	spoutConfig.startOffsetTime = OffsetRequest.EarliestTime();
	kafkaSpout = new KafkaSpout(spoutConfig);
	
	spoutConfig.scheme =  new SchemeAsMultiScheme(null);
	
	}
	
	public KafkaSpout getKafkaSpout(){
		return kafkaSpout;
	}
}
