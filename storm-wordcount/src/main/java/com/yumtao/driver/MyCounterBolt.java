package com.yumtao.driver;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MyCounterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private Map<String, Integer> counters = new HashMap<>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		
		System.out.println(String.format("couter bolt 》》 current thread is %d, word is %s", Thread.currentThread().getId(), word));
		if (!counters.containsKey(word)) {
			counters.put(word, 1);
		} else {
			counters.put(word, counters.get(word) + 1);
		}
//		System.out.println(counters);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
