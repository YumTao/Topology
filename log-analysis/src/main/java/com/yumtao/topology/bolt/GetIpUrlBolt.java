package com.yumtao.topology.bolt;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yumtao.common.BaseLog;
import com.yumtao.common.Const;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * @desc 解析bizMsg,分离出IP与URL
 * @in access ip: 180.169.135.186, url: /
 * @out [accessIp=180.169.135.186,accessUrl=/]
 * @author yumTao
 *
 */
public class GetIpUrlBolt extends BaseRichBolt {

	private final String ipDesc = "access ip: ";
	private final String urlDesc = "url: ";

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String bizMsg = input.getStringByField("bizMsg");
		BaseLog.getDailyLog().debug("Bolt: GetIpUrl READ tuple: {}", bizMsg);
		if (bizMsg.contains(ipDesc) && bizMsg.contains(urlDesc)) {
			Map<String, String> ip2UrlMap = getIpAndUrlMap(bizMsg);
			for (String ip : ip2UrlMap.keySet()) {
				List<Object> tuple = Arrays.asList(Arrays.asList(ip, ip2UrlMap.get(ip)).toArray());
				BaseLog.getDailyLog().info("Bolt: GetIpUrl WRITE tuple: {}", tuple);
				collector.emit(tuple);
			}
		}
	}

	/**
	 * @param bizMsg
	 * @return {ip=url}
	 */
	private Map<String, String> getIpAndUrlMap(String bizMsg) {
		Map<String, String> ip2Url = new HashMap<>();
		String removedIpDescStr = bizMsg.substring(bizMsg.indexOf(ipDesc) + ipDesc.length());
		String ip = removedIpDescStr.split(",")[0];
		String url = removedIpDescStr.substring(removedIpDescStr.indexOf(urlDesc) + urlDesc.length());
		ip2Url.put(ip, url);
		return ip2Url;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Const.GETIPURLBOLT_FIELD_IP, Const.GETIPURLBOLT_FIELD_URL));
	}

}
