package com.baidu.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

public class ConfUtils {
    public static Configuration getConf(String[] args) {
        Configuration conf = new Configuration(false);
        getConf(conf, args);
        return conf;
    }

    public static void getConf(Configuration conf,String[] args) {
        if (args == null) {
            return;
        }
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--conf".equals(arg) && i < args.length - 1) {
                String keyValue = args[i + 1];
                int index = keyValue.indexOf("=");
                if (index != -1 && index != keyValue.length() - 1) {
                    String key = keyValue.substring(0, index);
                    String value = keyValue.substring(index + 1);
                    conf.set(key, value);
                }
                i++;
            } else {
                File file = new File(arg);
                if (file.exists()) {
                    conf.addResource(new Path(arg));
                }
            }
        }
    }
}
