package com.yumtao.topo.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WriterBolt extends BaseRichBolt {

	private OutputCollector collector;

	private Map<String, Integer> result = new HashMap<>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Integer count = input.getIntegerByField("count");
		System.out.println(String.format("COUNT BOLT thread: %s, read is :%s", Thread.currentThread().getId(), word));
		int orgCount = result.get(word) == null ? 0 : result.get(word);
		result.put(word, count + orgCount);
		System.out.println(String.format("last word-count statistic Thread-Id is %s result is %s",
				Thread.currentThread().getId(), result));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {
		System.out.println(String.format("last word-count statistic Thread-Id is %s result is %s",
				Thread.currentThread().getId(), result));
	}

}
