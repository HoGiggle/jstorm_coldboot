package com.iflytek.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by yaowei on 2016/7/26.
 */
public class LogUtil {
    public static String getStackTraceString(Exception e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
//    private static LogUtil logUtil;
//    public static LogUtil getInstance(){
//         if(logUtil==null)
//             logUtil=new LogUtil();
//         return logUtil;
//    }
//    public void logE(Exception e){
//          logger.error(getStackTraceString(e));
//    }
//    public final  Logger logger;
//    private LogUtil(){
//                   logger= LoggerFactory.getLogger(this.getClass());
//    }

}
