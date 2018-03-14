package com.iflytek.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by yaowei on 2016/7/21.
 * todo ： maven 需要配置为shade plugin 才能读取hdfs
 */
public class HdfsUtil {
    public static String read(String path) throws IOException {
        Configuration conf = new Configuration();
        Path filePath = new Path(path);
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream fsdin = fileSystem.open(filePath);
        FileStatus status = fileSystem.getFileStatus(filePath);
        try {
            int len = (int) status.getLen();
            byte[] buffer = new byte[len];
            fsdin.readFully(0, buffer, 0, len);
            return new String(buffer);
        } finally {
            fsdin.close();
            fileSystem.close();
        }
    }


}
