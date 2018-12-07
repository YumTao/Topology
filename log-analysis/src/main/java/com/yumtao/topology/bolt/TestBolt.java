package com.yumtao.topology.bolt;

import java.util.Map;

import com.yumtao.common.log.BaseLog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TestBolt extends BaseRichBolt {

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {
		String string = input.getString(0);
		BaseLog.getDailyLog().info("TEST BOLT READ : {}", string);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
