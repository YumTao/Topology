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

/**
 * sport生命周期：declareOutputFields-> open -> nextTuple
 * 
 * @desc declareOutputFields: 声明sport输出tuple的field，加载sport时调用
 * @desc open: sport实例创建后的初始化方法
 * @desc nextTuple: 循环调用，给bolt发射tuple
 * @author yumTao
 *
 */
public class MyWordSport extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	
	// 自定义代码：sport自生成数据次数
	private int createSourceCounts = 10;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * @notice emit()方法输出的List与 declareOutputFields()声明的field索引上相对应
	 * @question emit()发射List为 [value1, value2], declareOutputFields()声明的field为[key1,key2]
	 * @result 最后发射的tuple为 [key1, value1][key2, value2]
	 */
	@Override
	public void nextTuple() {
		while (createSourceCounts > 0) {
			createSourceCounts--;

			List<Object> randomEng = getRandomEng();
			System.out.println(
					String.format("sport >> current thread is %d, write is %s", Thread.currentThread().getId(), randomEng));
			collector.emit(randomEng);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	public List<Object> getRandomEng() {
		List<String> engList = new ArrayList<>();
		engList.add("I am very strong yes");

		Random random = new Random();
		int nextInt = random.nextInt(engList.size());

		List<Object> result = new ArrayList<>();
		result.add(engList.get(nextInt));
		return result;
	}

}
