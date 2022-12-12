package com.bigdata.hisign.sparkstreaming.util;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

/**
 * 累加器
 *
 * @author zhangguoqing
 * @classname JavaDroppedWordsCounter
 * @date 2022年12月7日18:30:27
 */
public class JavaDroppedWordsCounter {
    private static volatile LongAccumulator instance = null;

    public static LongAccumulator getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaDroppedWordsCounter.class) {
                if (instance == null) {
                    instance = jsc.sc().longAccumulator("WordsInBlacklistCounter");
                }
            }
        }
        return instance;
    }
}
