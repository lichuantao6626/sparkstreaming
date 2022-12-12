package com.bigdata.hisign.sparkstreaming.stream;

import com.alibaba.fastjson.JSON;
import com.zgq.spark.model.Person;
import com.zgq.spark.util.JavaSparkSessionSingleton;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * kafka集成Streaming
 *
 * @classname KafkaStream
 * @date 2022年12月7日18:26:18
 */
public class KafkaStream {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaStream");
        SparkSession spark = JavaSparkSessionSingleton.getInstance(sparkConf);
        System.out.println(spark);
        spark.sql("insert into table xzxtdwd.user_test values ('name100','30')");
        JavaStreamingContext streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(2));
//        // 设置检查点，检查点目录一般设置为hdfs上的一个目录，提高数据安全性
        streamingContext.checkpoint("./checkpoint");
        // kafka topics订阅set
        Set<String> topicsSet = new HashSet<>();
        topicsSet.add("kafkaStreaming");
        // kafka配置参数
        Map<String, Object> kafkaParams = new HashMap<>(16);
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cn75.hadoop.hisign:9092,cn76.hadoop.hisign:9092,cn77.hadoop.hisign:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-streaming-test");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 创建kafka stream 订阅参数配置
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
        JavaDStream<String> lines = messages.map((Function<ConsumerRecord<String, String>, String>) record -> {
            System.out.println("key:" + record.key() + ", value: " + record.value() + ", topic: " + record.topic());
            return record.value();
        });
        lines.print();
        lines.foreachRDD(rdd -> {
            if (rdd.count() > 0) {
                SparkSession session = JavaSparkSessionSingleton.getInstance(rdd.context().conf());
                JavaRDD<Person> person = rdd.map(JSON::parseObject).filter(x -> x.containsKey("username"))
                        .map(x -> new Person(x.getString("username"), x.getInteger("age")));
                Dataset<Row> dataset = session.createDataFrame(person, Person.class);
                dataset.createOrReplaceTempView("temp");
                session.sql("insert into table xzxtdwd.user_test select username,age from temp");
            }
        });
        // 开始计算
        streamingContext.start();
        // 等待计算完成
        streamingContext.awaitTermination();
        streamingContext.stop(true, true);
    }
}
