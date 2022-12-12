package com.bigdata.hisign.sparkstreaming.util;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Broadcast广播变量实例
 *
 * @author zhangguoqing
 * @classname JavaWordBlacklist
 * @date 2022年12月7日18:30:22
 */
public class JavaWordBlacklist {
    private static volatile Broadcast<List<String>> instance = null;

    public static Broadcast<List<String>> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaWordBlacklist.class) {
                if (instance == null) {
                    List<String> wordBlacklist = Arrays.asList("one", "the", "license");
                    instance = jsc.broadcast(wordBlacklist);
                }
            }
        }
        return instance;
    }
}
