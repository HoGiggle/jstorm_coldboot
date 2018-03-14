package com.iflytek.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

/**
 * Created by yaowei on 2016/7/21.
 */
public class TimeFunction {
    public static String getCurrentTime(String format){
        return new SimpleDateFormat(format).format(Calendar.getInstance().getTime());
    }
    public static long getTimestamp(String time, String timeFormat) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat(timeFormat, Locale.US);
        return df.parse(time).getTime();
    }
}
