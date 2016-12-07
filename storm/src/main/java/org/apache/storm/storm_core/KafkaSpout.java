package org.apache.storm.storm_core;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaSpout {
	BrokerHosts hosts = new ZkHosts(zkConnString);
	SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
	spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

}
