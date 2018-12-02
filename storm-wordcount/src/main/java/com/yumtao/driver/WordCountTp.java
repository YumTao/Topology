package com.yumtao.driver;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTp {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("productWordSpt", new MyWordSport(), 2);
		builder.setBolt("splitBolt", new MyWordSplitBolt(), 2).shuffleGrouping("productWordSpt");
		builder.setBolt("counterBolt", new MyCounterBolt(), 4).fieldsGrouping("splitBolt", new Fields("word"));

		StormTopology wordcountTopology = builder.createTopology();

		Config config = new Config();
		// config.setDebug(true);

		config.setNumWorkers(2);

		// 3.提交任务两种方式，集群模式和本地模式
		StormSubmitter.submitTopology("taowordcount", config, wordcountTopology);

		// LocalCluster localCluster = new LocalCluster();
		// localCluster.submitTopology("taowordcount", config, wordcountTopology);
		// Utils.sleep(100000);
		// localCluster.killTopology("taowordcount");
		// localCluster.shutdown();

		// LocalCluster localCluster = new LocalCluster();
		// localCluster.submitTopology("taowordcount", config,
		// builder.createTopology());
	}

}
