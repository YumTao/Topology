package com.yumtao.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.yumtao.common.log.BaseLog;

/**
 * 日期工具类
 *
 * @author YQT
 */
public class DateUtil {

	private static SimpleDateFormat SRC_SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static SimpleDateFormat TAR_SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * 获取两个时间的间隔毫秒值
	 *
	 * @param begin 开始时间
	 * @param end   结束时间
	 * @return
	 */
	public static long getMillSecBetween(Date begin, Date end) {
		return end.getTime() - begin.getTime();
	}

	public static String format(Date date) {
		return TAR_SDF.format(date);
	}

	public static Date parse(String dateStr) {
		try {
			return SRC_SDF.parse(dateStr);
		} catch (ParseException e) {
			BaseLog.getErrorLog().error(String.format("parse date error, date: %s"), dateStr);
		}
		return null;
	}

	// 获取当天的最开始时间 00:00:00 000
	public static Date getStartTime(Date date) {

		if (date == null) {
			return null;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.MILLISECOND, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		Date time = calendar.getTime();
		return time;
	}

}
