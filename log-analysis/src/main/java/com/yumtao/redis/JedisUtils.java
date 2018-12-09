package com.yumtao.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * jedis 工具类
 * 
 * @author yumtao
 *
 */
public class JedisUtils {
	// 创建连接池
	private static JedisPoolConfig config;
	private static JedisPool pool;
	
	public static final String KEY_URLCOUNTBYIP_PREFIX = "blob:statistic:access:urlCount:ip:";
	public static final String KEY_URLCOUNT_PREFIX = "blob:statistic:access:urlCount:url:";

	// 配置连接池
	static {
		// 创建连接池对象
		config = new JedisPoolConfig();
		// 设置连接最大个数
		config.setMaxTotal(30);
		// 设置最大空闲个数
		config.setMaxIdle(2);

		// 获得连接池对象
		pool = new JedisPool(config, "singlenode", 6379);
	}

	// 获取连接对象
	public static Jedis getJedis() {
		return pool.getResource();
	}

	// 释放资源
	public static void release(Jedis jedis) {
		if (jedis != null) {
			jedis.close();
		}
	}

}
