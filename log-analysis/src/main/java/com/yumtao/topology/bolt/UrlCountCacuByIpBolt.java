package com.yumtao.topology.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
 * @desc 统计单个ip的url访问次数
 * @eg ip: {url:count,...}
 * @author yumTao
 *
 */
public class UrlCountCacuByIpBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private Jedis jedis;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		jedis = JedisUtils.getJedis();
	}

	@Override
	public void execute(Tuple input) {
		String ip = input.getStringByField(Const.GETIPURLBOLT_FIELD_IP);
		String url = input.getStringByField(Const.GETIPURLBOLT_FIELD_URL);
		BaseLog.getDailyLog().info("BOLT: url count by ip, id={}, ip= {}, url= {}", Thread.currentThread().getId(), ip, url);

		// 更新当前ip的url count map
		Map<String, Integer> url2Count = getUrl2CountFromIp(ip);
		BaseLog.getDailyLog().info("BOLT: url count by ip, redis GET: {}={}", JedisUtils.KEY_URLCOUNTBYIP_PREFIX + ip, url2Count);
		int count = url2Count.get(url) == null ? 0 : url2Count.get(url);
		url2Count.put(url, ++count);

		String sinkUrl2Count = JSON.toJSONString(url2Count);
		jedis.set(JedisUtils.KEY_URLCOUNTBYIP_PREFIX + ip, sinkUrl2Count);
		BaseLog.getDailyLog().info("BOLT: url count by ip, redis SET: {}={}", JedisUtils.KEY_URLCOUNTBYIP_PREFIX + ip, sinkUrl2Count);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Integer> getUrl2CountFromIp(String ip) {
		Map<String, Integer> url2Count = new HashMap<>();
		String url2CountStr = jedis.get(JedisUtils.KEY_URLCOUNTBYIP_PREFIX + ip);
		if (StringUtils.isNotEmpty(url2CountStr)) {
			url2Count = (Map<String, Integer>) JSONObject.parse(url2CountStr);
		}
		return url2Count;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {
		jedis.close();
	}

}
