package com.yumtao.driver;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Topology任务注册类
 * 1.指定sport实例
 * 2.指定bolt实例
 * 3.提交任务
 * @author yumTao
 *
 */
public class WordCountTp {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		// 设置sport（id, sport实例，并发度）
		builder.setSpout("productWordSpt", new MyWordSport(), 2);
		// 设置bolt,并制定分组策略，入参为输入的对应id，这里是sport的id
		builder.setBolt("splitBolt", new MyWordSplitBolt(), 2).shuffleGrouping("productWordSpt");
		// 设置bolt,并制定分组策略，入参为输入的对应id，这里是上一个bolt的id
		builder.setBolt("counterBolt", new MyCounterBolt(), 4).fieldsGrouping("splitBolt", new Fields("word"));

		StormTopology wordcountTopology = builder.createTopology();

		Config config = new Config();

		// 设置worker数
		config.setNumWorkers(2);

		// 3.提交任务两种方式，集群模式和本地模式
		// 集群模式
		// StormSubmitter.submitTopology("taowordcount", config, wordcountTopology);

		// 本地模式
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("taowordcount", config, builder.createTopology());
	}

}
