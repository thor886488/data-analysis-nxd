package com.analysis.nxd.common.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * 日期公用类
 */
public class DateUtils {
    public static final String MONTH_SHORT_FORMAT = "yyyy-MM";
    public static final String DATE_SHORT_FORMAT = "yyyy-MM-dd";
    public static final String DATE_MM_FORMAT = "yyyy-MM-dd HH:mm";
    public static final String DATE_FULL_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_NUM_FULL_FORMAT = "yyyyMMddHHmmss";
    public static final String DATE_UTE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String DATE_TIGHT_FORMAT = "yyyyMMddHHmm";
    public static final String DATE_NUMBER_FORMAT = "yyyyMMdd";
    public static final String HOUR_SHORT_FORMAT = "HH";

    /**
     * 创建日期格式化实现类
     *
     * @param pattern 指定的日期转换格式
     * @return 日期格式化实现类
     */
    private static DateFormat getDateFormat(String pattern) {
        return new SimpleDateFormat(pattern);
    }

    /**
     * 把字符传解析成日期
     *
     * @param time
     * @return
     */
    public static Date parseDate(String time, String pattern) {
        Date date = null;
        try {
            date = getDateFormat(pattern).parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 把日期格式化为字符串
     *
     * @param date
     * @return
     * @throws ParseException
     */
    public static String formatDate(Date date, String pattern) {
        return getDateFormat(pattern).format(date);
    }

    /**
     * 把字符先解析成日期，再格式化成字符串
     *
     * @param time
     * @return
     * @throws ParseException
     */
    public static String parseFormatDate(String time, String pattern1, String pattern2) {
        String Strdate = null;
        try {
            Date date = getDateFormat(pattern1).parse(time);
            Strdate = getDateFormat(pattern2).format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return Strdate;
    }

    /**
     * 获取系统时间
     */
    public static String getSysFullDate() {
        Calendar calendar = Calendar.getInstance();
        return getDateFormat(DATE_FULL_FORMAT).format(calendar.getTime());
    }

    /**
     * 获取系统时间
     */
    public static String getSysFullDateNUmber() {
        Calendar calendar = Calendar.getInstance();
        return getDateFormat(DATE_NUM_FULL_FORMAT).format(calendar.getTime());
    }

    /**
     * 获取系统日期"yyyy-MM-dd"（cyz于20100319增加）
     */
    public static String getSysMin() {
        Calendar calendar = Calendar.getInstance();
        return getDateFormat(DATE_MM_FORMAT).format(calendar.getTime());
    }

    /**
     * 获取系统日期"yyyy-MM-dd"（cyz于20100319增加）
     */
    public static Integer getSysHour() {
        Calendar calendar = Calendar.getInstance();
        return Integer.valueOf(getDateFormat(HOUR_SHORT_FORMAT).format(calendar.getTime()));
    }

    /**
     * 获取系统日期"yyyy-MM-dd"（cyz于20100319增加）
     */
    public static String getSysDate() {
        Calendar calendar = Calendar.getInstance();
        return getDateFormat(DATE_SHORT_FORMAT).format(calendar.getTime());
    }


    /**
     * 获取某个日期所在月的第一天
     * 注意是自然周，比如传入2018-11-07 00:00:00 则返回2018-11-01 00:00:00
     */
    public static String getFirstDayOfMonth(String time) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(getDateFormat(DATE_SHORT_FORMAT).parse(time));
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            return getDateFormat(DATE_SHORT_FORMAT).format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获得某个日期所在月的最后一天
     * 注意是自然周，比如传入2018-11-07 00:00:00 则返回2018-11-30 00:00:00
     */
    public static String getLastDayOfMonth(String time) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(getDateFormat(DATE_SHORT_FORMAT).parse(time));
            calendar.add(Calendar.MONTH, 1);
            calendar.set(Calendar.DAY_OF_MONTH, 0);
            return getDateFormat(DATE_SHORT_FORMAT).format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取某个日期所在星期的第一天
     * 注意是自然周，比如传入2018-11-07 00:00:00 则返回2018-11-01 00:00:00
     */
    public static String getFirstDayOfWeek(String time) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(getDateFormat(DATE_SHORT_FORMAT).parse(time));
            calendar.set(Calendar.DAY_OF_WEEK, 2);
            return getDateFormat(DATE_SHORT_FORMAT).format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获得某个日期所在星期的最后一天
     * 注意是自然周，比如传入2018-11-07 00:00:00 则返回2018-11-30 00:00:00
     */
    public static String getLastDayOfWeek(String time) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(getDateFormat(DATE_SHORT_FORMAT).parse(time));
            calendar.set(Calendar.DAY_OF_WEEK, 2);
            calendar.add(Calendar.DATE, 6);
            return getDateFormat(DATE_SHORT_FORMAT).format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @param time
     * @param day
     * @return
     */
    public static String addDay(String time, Integer day) {
        return formatDate(add(parseDate(time, DATE_SHORT_FORMAT), Calendar.DATE, day), DATE_SHORT_FORMAT);
    }

    /**
     * @param time
     * @param second
     * @return
     */
    public static String addSecond(String time, Integer second) {
        return formatDate(add(parseDate(time, DATE_FULL_FORMAT), Calendar.SECOND, second), DATE_FULL_FORMAT);
    }

    /**
     * @param time
     * @param second
     * @return
     */
    public static String addSecond(String time, Integer second, String pattern) {
        return formatDate(add(parseDate(time, DATE_FULL_FORMAT), Calendar.SECOND, second), pattern);
    }

    /**
     * @param time
     * @param day
     * @return
     */
    public static String addMonth(String time, Integer day) {
        return formatDate(add(parseDate(time, MONTH_SHORT_FORMAT), Calendar.MONTH, day), MONTH_SHORT_FORMAT);
    }

    /**
     * 指定日期增加（年）
     *
     * @param date   指定的一个原始日期
     * @param amount 数值增量
     * @return 新日期
     */
    public static Date addYear(Date date, int amount) {
        return add(date, Calendar.YEAR, amount);
    }

    /**
     * 指定日期增加数量（年，月，日，小时，分钟，秒，毫秒）
     *
     * @param date   指定的一个原始日期
     * @param field  日历类Calendar字段
     * @param amount 数值增量
     * @return
     */
    public static Date add(Date date, int field, int amount) {
        if (date == null) {
            throw new IllegalArgumentException("The date must not be null");
        } else {
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            c.add(field, amount);
            return c.getTime();
        }
    }

    /**
     * 比较两个日期的大小
     *
     * @param pre
     * @param after
     * @return 天  时间差
     */
    public static long compareDate(String pre, String after) {
        DateFormat df = getDateFormat(DateUtils.DATE_SHORT_FORMAT);
        try {
            Date preD = df.parse(pre);
            Date afterD = df.parse(after);
            return (preD.getTime() - afterD.getTime()) / (3600 * 24 * 1000);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return 0;
    }

    /**
     * 比较两个时间的大小
     *
     * @param pre
     * @param after
     * @return 秒时间差
     */
    public static long compareTime(String pre, String after) {
        DateFormat df = getDateFormat(DateUtils.DATE_FULL_FORMAT);
        try {
            Date preD = df.parse(pre);
            Date afterD = df.parse(after);
            return preD.getTime() - afterD.getTime();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return 0;
    }


    /**
     * 判断两个日期相差几个月
     *
     * @param startDate
     * @param endDate
     * @return
     */
    public static Integer differentMonth(String startDate, String endDate) {
        Calendar start = Calendar.getInstance();
        Calendar end = Calendar.getInstance();
        start.setTime(parseDate(startDate, DATE_SHORT_FORMAT));
        end.setTime(parseDate(endDate, DATE_SHORT_FORMAT));
        int result = end.get(Calendar.MONTH) - start.get(Calendar.MONTH);
        int month = (end.get(Calendar.YEAR) - start.get(Calendar.YEAR)) * 12;
        return Math.abs(month + result);
    }

    /**
     * 判断两个日期相差多少天
     * differentDays("2018-10-01","2018-10-02","yyyy-MM-dd")  结果：1
     */
    public static int differentDays(String date1, String date2, String pattern) {
        Date d1 = parseDate(date1, pattern);
        Date d2 = parseDate(date2, pattern);
        int days = (int) ((d2.getTime() - d1.getTime()) / (1000 * 3600 * 24l));
        return days;
    }

    /**
     * 获取日期是星期几(0~6,0为星期日)
     *
     * @param time 指定的一个原始日期
     * @return
     */
    public static int getWeekOfDate(String time) {
        int week = 1;
        try {
            Calendar calendar = Calendar.getInstance();
            Date date = getDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
            calendar.setTime(date);
            week = calendar.get(Calendar.DAY_OF_WEEK) - 1;
            if (week == 0) {
                week = 7;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return week;
    }

    /**
     * 获取数据开始时间
     *
     * @return
     */
    public static String getStartDate() {
        return addSecond(getSysFullDate(), -3600 * 13, DateUtils.DATE_MM_FORMAT).substring(0, 10) + " 00:00:00";
    }

    /**
     * 获取数据开始时间
     *
     * @return
     */
    public static String getStartHourDate() {
        return addSecond(getSysFullDate(), -3600 * 2, DateUtils.DATE_MM_FORMAT).substring(0, 14) + "00:00";
    }

    /**
     * 获取数据开始时间
     *
     * @return
     */
    public static String getStartHourDateBM() {
        return addSecond(getSysFullDate(), -3600 * 12, DateUtils.DATE_MM_FORMAT).substring(0, 14) + "00:00";
    }

    /**
     * 获取数据开始时间
     *
     * @return
     */
    public static String getThirdlyAppStartDate() {
        return addSecond(getSysFullDate(), -3600 * 72, DateUtils.DATE_MM_FORMAT).substring(0, 10) + " 00:00:00";
    }

    /**
     * 获取数据开始时间
     *
     * @return
     */
    public static String getTodayStartDate() {
        return getSysFullDate().substring(0, 10) + " 00:00:00";
    }

    /**
     * 获取数据结束时间
     *
     * @return
     */
    public static String getEndDate() {
        return addSecond(getSysFullDate(), -3600, DateUtils.DATE_MM_FORMAT).substring(0, 13) + ":59:59";
    }

    /**
     * 获取数据结束时间
     *
     * @return
     */
    public static String getEndHourDate() {
        return addSecond(getSysFullDate(), 0, DateUtils.DATE_MM_FORMAT).substring(0, 15) + "9:59";
    }

    /**
     * 获取数据结束时间
     *
     * @return
     */
    public static String getTodayEndDate() {
        return getSysFullDate().substring(0, 10) + " 23:59:59";
    }


    /**
     * 把 类型为 Timestamp 格式为 yyyy-MM-dd HH:mm:ss 的 字符串转化为 doris odbc to  oracle 可以识别的时间格式
     *
     * @param strDate
     * @return
     */
    public static String tranDorisToOracleTimestamp(String strDate) {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "Jan");
        map.put(2, "Feb");
        map.put(3, "Mar");
        map.put(4, "Apr");
        map.put(5, "May");
        map.put(6, "Jun");
        map.put(7, "Jul");
        map.put(8, "Aug");
        map.put(9, "Sep");
        map.put(10, "Oct");
        map.put(11, "Nov");
        map.put(12, "Dec");
        Calendar c = Calendar.getInstance();
        c.setTime(parseDate(strDate, DATE_FULL_FORMAT));
        int day = c.get(c.DAY_OF_MONTH);
        int month = c.get(c.MONTH) + 1;
        int year = c.get(c.YEAR);
        int hour = c.get(c.HOUR_OF_DAY);
        int minute = c.get(c.MINUTE);
        int second = c.get(c.SECOND);

        return (day + "-" + map.get(month) + "-" + (year + "").substring(2, 4) + " " + ((hour % 12) == 0 ? 12 : (hour % 12)) + "." + minute + "." + second + ".000000 " + ((hour / 12) == 0 ? "AM" : "PM"));

    }

    public static void main(String[] args) {

        System.out.println(tranDorisToOracleTimestamp("2022-07-02 23:59:59"));
    }

}