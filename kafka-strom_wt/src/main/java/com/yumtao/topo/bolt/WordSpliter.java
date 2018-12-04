package com.yumtao.topo.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WordSpliter extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String line = input.getValue(0).toString().trim();
		if (StringUtils.isEmpty(line)) {
			return;
		}
//		String line = new String((byte[]) input.getValue(0));
		System.out.println(String.format("SPLIT BOLT read data %s", line));
		Map<String, Integer> word2Count = new HashMap<>();
		Arrays.asList(line.split(" ")).stream().forEach(word -> {
			int count = word2Count.get(word) == null ? 0 : word2Count.get(word);
			word2Count.put(word, ++count);
		});

		for (String word : word2Count.keySet()) {
			List<Object> tuple = new ArrayList<>();
			tuple.add(word);
			tuple.add(word2Count.get(word));
			collector.emit(tuple);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
