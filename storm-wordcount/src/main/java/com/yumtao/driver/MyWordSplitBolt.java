package com.yumtao.driver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MyWordSplitBolt extends BaseRichBolt {

	private OutputCollector collector;
	
	private List<Object> outObj;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String engs = input.getString(0);
		System.out.println(String.format("split bolt >> current thread is %d, word is %s", Thread.currentThread().getId(), engs));
		Arrays.asList(engs.split(" ")).stream().forEach(word -> {
			outObj = new ArrayList<>();
			outObj.add(word);
			outObj.add(1);
			
//			System.out.println("split bolt write : " + outObj);
			collector.emit(outObj);
		});
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));

	}

}
