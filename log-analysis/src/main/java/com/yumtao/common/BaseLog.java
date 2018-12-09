package com.yumtao.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseLog {
    
	/**
	 * 每日一个文件的业务记录日志
	 * 每天会生成一个新的log文件.请根据业务需要使用
	 */
    private static Logger dailyLog = LoggerFactory.getLogger("DailyLog");
	public static Logger getDailyLog() {
		return dailyLog;
	}
	
	/**
	 * 系统错误日志
	 * 记录各种异常错误,只提供error和warn
	 */
	private static Logger errorLog = LoggerFactory.getLogger("ErrorLog");
	public static Logger getErrorLog() {
		return errorLog;
	}
	
	/**
	 * 系统日志
	 * 记录系统启动,参数,运行状态等.
	 */
	private static Logger systemLog = LoggerFactory.getLogger("SystemLog");
	public static Logger getSystemLog() {
		return systemLog;
	}
}
