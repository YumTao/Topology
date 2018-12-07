package com.yumtao.topology;

import com.yumtao.topology.bolt.BizMsgSplitBolt;
import com.yumtao.topology.bolt.GetIpUrlBolt;
import com.yumtao.topology.bolt.BizMsgBolt;
import com.yumtao.topology.bolt.TestBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaTopo {

	private static String ZK_HOST = "singlenode:2181";
	private static String TOPIC = "first";// 执行要消费的topic
//	private static String TOPIC = "blob-log-stream-topic";// 执行要消费的topic
	private static String ZK_ROOT = "/blob-log-offset";// zk上管理offset的位置(随便定义)

	private static String TP_ID = "blog-log-analysis";
	private static String SPOUT_ID = "blog-log-sport";
	private static String PRE_BOLT_ID = "blog-log-split";
	private static String SPLIT2IP = "get-ip-bolt";

	public static void main(String[] args) throws Exception {
		BrokerHosts brokerHosts = new ZkHosts(ZK_HOST);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, TOPIC, ZK_ROOT, SPOUT_ID);
//		spoutConfig.forceFromStart = true;// 从头开始读
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SPOUT_ID, kafkaSpout);
		builder.setBolt(PRE_BOLT_ID, new BizMsgBolt(), 1).shuffleGrouping(SPOUT_ID);
		builder.setBolt(SPLIT2IP, new GetIpUrlBolt(), 1).shuffleGrouping(PRE_BOLT_ID);
		builder.setBolt("test", new TestBolt(), 1).shuffleGrouping(SPOUT_ID);
//		builder.setBolt(BIZMSG_SPLIT_BOLT_ID, new BizMsgSplitBolt(), 1).shuffleGrouping(PRE_BOLT_ID);

		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.setNumAckers(0);
		conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TP_ID, conf, builder.createTopology());
	}
}