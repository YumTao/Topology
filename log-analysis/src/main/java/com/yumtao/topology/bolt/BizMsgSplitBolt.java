package com.yumtao.topology.bolt;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yumtao.common.log.BaseLog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class BizMsgSplitBolt extends BaseRichBolt {
	
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String bizMsg = input.getStringByField("bizMsg");
		BaseLog.getDailyLog().info("BizMsgSplitBolt READ tuple: {}", bizMsg);
		if (StringUtils.isEmpty(bizMsg)) {
			return;
		}
		if (bizMsg.contains("access ip")) {
			System.out.println("bizmsg: " + bizMsg);
		}
		//		collector.emitDirect(taskId, tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields(fields));
	}

}
