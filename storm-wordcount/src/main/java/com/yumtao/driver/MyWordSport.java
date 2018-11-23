package com.yumtao.driver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class MyWordSport extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		List<Object> randomEng = getRandomEng();
		System.out.println("sport write : " + randomEng);
		collector.emit(randomEng);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	public List<Object> getRandomEng() {
		List<String> engList = new ArrayList<>();
		engList.add("I am strong");
		engList.add("I come from CN");
		engList.add("Life is so nice");
		engList.add("oh yes");
		engList.add("oh lei oh lei");

		Random random = new Random();
		int nextInt = random.nextInt(5);

		List<Object> result = new ArrayList<>();
		result.add(engList.get(nextInt));
		return result;
	}

}