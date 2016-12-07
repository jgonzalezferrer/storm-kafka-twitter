package org.apache.storm.storm_core;

import java.util.UUID;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

public class KafkaTwitterSpout {
	public KafkaTwitterSpout(String zkConn, String topicName){
	BrokerHosts hosts = new ZkHosts(zkConn);
	SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
	spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
	}
}
