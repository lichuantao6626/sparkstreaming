package com.bigdata.hisign.sparkstreaming.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * JavaSparkSession 单例
 *
 * @author zhangguoqing
 * @classname JavaSparkSessionSingleton
 * @date 2019年10月17日 16:32
 */
public class JavaSparkSessionSingleton {

    /**
     * SparkSession实例
     */
    private static transient SparkSession instance = null;

    /**
     * SparkSession
     *
     * @param sparkConf sparkConf配置
     * @return SparkSession单例
     * @author zhangguoqing
     * @date 2019年10月17日16:34:11
     */
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        }
        return instance;
    }

    public static SparkSession getSparkSession() {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .appName("RybqMainJob")
//                    .master("local[2]")
                    .enableHiveSupport()
                    .getOrCreate();
        }
        return instance;
    }
}
