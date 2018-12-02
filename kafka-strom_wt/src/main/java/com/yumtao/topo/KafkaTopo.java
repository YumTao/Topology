package com.yumtao.topo;

import com.yumtao.topo.bolt.WordSpliter;
import com.yumtao.topo.bolt.WriterBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaTopo {

	private static String ZK_HOST = "master2:2181,master2-standby:2181,master-standby:2181";
//	private static String ZK_HOST = "singlenode:2181";
	private static String TOPIC = "first";// 执行要消费的topic
	private static String ZK_ROOT = "/kafka-offset";// zk上管理offset的位置

	private static String TP_ID = "kafkaStrom-wordCount";// topology ID
	private static String SPOUT_ID = "kafkaStrom-sport";// sport ID
	private static String PRE_BOLT_ID = "word-spliter";// bolt ID
	private static String OUT_BOLT_ID = "word-count";// bolt ID

	public static void main(String[] args) throws Exception {
		// 这里相当于一个消费者，所以不知道topic所在的broker是那台，我们只需要指定zk即可， 从zk中取的元数据查看
		// topic所在broker，从而拿到topic中生产者的数据
		// 实际开发生产者一般用flume采集数据，之后文章介绍 flume整合kafka

		
		BrokerHosts brokerHosts = new ZkHosts(ZK_HOST);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, TOPIC, ZK_ROOT, SPOUT_ID);
//		spoutConfig.forceFromStart = true;// 从头开始读
//		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConfig));
		builder.setBolt(PRE_BOLT_ID, new WordSpliter()).shuffleGrouping(SPOUT_ID);
		builder.setBolt(OUT_BOLT_ID, new WriterBolt(), 4).fieldsGrouping(PRE_BOLT_ID, new Fields("word"));
		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.setNumAckers(0);
		conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		// LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
		cluster.submitTopology(TP_ID, conf, builder.createTopology());

		// 提交topology到storm集群中运行
		// StormSubmitter.submitTopology("kafkaStrom-topo", conf,
		// builder.createTopology());
	}
}