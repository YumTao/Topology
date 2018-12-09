package com.yumtao.topology.bolt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yumtao.common.BaseLog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * @desc 获取业务信息，并发送出去
 * @in 2018-12-06 15:49:06.722 INFO [cn.yqt.springmvc.web.filter.ForCrawlerFilter.doFilter:58] access ip: 180.169.135.186, url: /
 * @out access ip: 180.169.135.186, url: /
 * @author yumTao
 *
 */
public class BizMsgBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String logLine = input.getString(0);
		BaseLog.getDailyLog().info("Bolt: BizMsg READ tuple: {}", logLine);
		if (StringUtils.isEmpty(logLine)) {
			return;
		}

		List<Object> tuple = getTupleFromLine(logLine);
		if (null != tuple) {
			BaseLog.getDailyLog().info("Bolt: BizMsg WRITE tuple: {}", tuple);
			collector.emit(tuple);
		}

	}

	private List<Object> getTupleFromLine(String logLine) {
		List<Object> tuple = null;
		try {
			List<String> logMsgs = Arrays.asList(logLine.split(" "));
			String bizMsg = "";
			for (int i = 4; i < logMsgs.size(); i++) {
				bizMsg += " " + logMsgs.get(i);
			}

			tuple = Arrays.asList(Arrays.asList(bizMsg).toArray());
		} catch (Exception e) {
			BaseLog.getDailyLog().info("not common type log: {}, emit it on same in", logLine);
		}

		return tuple;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("bizMsg"));
	}

}
