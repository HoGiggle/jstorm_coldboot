package com.iflytek.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

/**
 * Created by yaowei on 2016/7/20.
 */
public class LocalConfHelper {
    public static Config getConfig(String confPath){
       return  ConfigFactory.load(ConfigFactory.parseFile(new File(confPath)));
    }
}
