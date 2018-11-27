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

/**
 * bolt 声明周期 declareOutputFields -> prepare -> execute
 * 
 * @desc declareOutputFields: 声明bolt输出tuple的field，加载bolt时调用
 * @desc prepare: bolt实例创建后的初始化方法
 * @desc execute: 循环调用，处理发送过来的tuple
 * @author yumTao
 *
 */
public class MyWordSplitBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	private List<Object> outObj;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * @notice emit()方法输出的List与 declareOutputFields()声明的field索引上相对应
	 * @question emit()发射List为【value1， value2】， declareOutputFields()声明的field为【key1， key2】
	 * @result 最后发射的tuple为 【key1， value1】【key2， value2】
	 */
	@Override
	public void execute(Tuple input) {
		String engs = input.getString(0);
		System.out.println(
				String.format("split bolt >> current thread is %d, word is %s", Thread.currentThread().getId(), engs));
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
