package com.yumtao.topology;

import com.yumtao.common.Const;
import com.yumtao.topology.bolt.BizMsgBolt;
import com.yumtao.topology.bolt.GetIpUrlBolt;
import com.yumtao.topology.bolt.UrlCountCacuBolt;
import com.yumtao.topology.bolt.UrlCountCacuByIpBolt;

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

	private static String ZK_HOST = "singlenode:2181";
	private static String TOPIC = "blob-log-stream-topic";// 执行要消费的topic
	private static String ZK_ROOT = "/blob-log-offset";// zk上管理offset的位置(随便定义)

	private static String TP_ID = "blog-log-analysis";
	private static String SPOUT_ID = "getSrc-sport";
	private static String GET_BIZMSG_BOLT_ID = "getBizMsg-bolt";
	private static String GET_IP_URL_BOLT_ID = "get-ip-url-bolt";
	private static String URLCOUNT_BYIP_BOLT_ID = "urlcountcaculate-by-ip-bolt";
	private static String URLCOUNT_BOLT_ID = "urlcountcaculate-all-bolt";

	public static void main(String[] args) throws Exception {
		BrokerHosts brokerHosts = new ZkHosts(ZK_HOST);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, TOPIC, ZK_ROOT, SPOUT_ID);
//		spoutConfig.forceFromStart = true;// 从头开始读
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SPOUT_ID, kafkaSpout);
		builder.setBolt(GET_BIZMSG_BOLT_ID, new BizMsgBolt(), 2).shuffleGrouping(SPOUT_ID);
		builder.setBolt(GET_IP_URL_BOLT_ID, new GetIpUrlBolt(), 2).shuffleGrouping(GET_BIZMSG_BOLT_ID);
		builder.setBolt(URLCOUNT_BYIP_BOLT_ID, new UrlCountCacuByIpBolt(), 2).fieldsGrouping(GET_IP_URL_BOLT_ID,
				new Fields(Const.GETIPURLBOLT_FIELD_IP));
		builder.setBolt(URLCOUNT_BOLT_ID, new UrlCountCacuBolt(), 2).fieldsGrouping(GET_IP_URL_BOLT_ID,
				new Fields(Const.GETIPURLBOLT_FIELD_URL));

		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.setNumAckers(0);
		conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TP_ID, conf, builder.createTopology());
		
		// 提交topology到storm集群中运行
		// StormSubmitter.submitTopology(TP_ID, conf,
		// builder.createTopology());
	}
}