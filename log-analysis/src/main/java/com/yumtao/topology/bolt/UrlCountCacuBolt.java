package com.yumtao.topology.bolt;

import java.util.Map;

import com.yumtao.common.BaseLog;
import com.yumtao.common.Const;
import com.yumtao.redis.JedisUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

/**
 * @desc 统计url访问次数
 * @eg url: count
 * @author yumTao
 *
 */
public class UrlCountCacuBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private Jedis jedis;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		jedis = JedisUtils.getJedis();
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField(Const.GETIPURLBOLT_FIELD_URL);
		BaseLog.getDailyLog().info("BOLT: url count, id={}, url= {}", Thread.currentThread().getId(), url);
		String countStr = jedis.get(JedisUtils.KEY_URLCOUNT_PREFIX + url);
		int count = countStr == null ? 0 : Integer.valueOf(countStr);
		BaseLog.getDailyLog().info("BOLT: url count, redis GET: {}={}", JedisUtils.KEY_URLCOUNT_PREFIX + url, count);
		
		jedis.set(JedisUtils.KEY_URLCOUNT_PREFIX + url, String.valueOf(++count));
		BaseLog.getDailyLog().info("BOLT: url count, redis SET: {}={}", JedisUtils.KEY_URLCOUNT_PREFIX + url, count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
