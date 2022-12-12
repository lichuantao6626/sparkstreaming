package com.zgq.spark.stream;

import com.google.common.io.Files;
import com.zgq.spark.util.JavaDroppedWordsCounter;
import com.zgq.spark.util.JavaWordBlacklist;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 第14篇 检查点
 *
 * @author zhangguoqing
 * @classname Learn15CheckPoint
 * @date 2019年10月22日 11:01
 */
public class CheckPoint {

    private static final Pattern SPACE = Pattern.compile(" ");

    private static JavaStreamingContext createContext(String ip,
                                                      int port,
                                                      String checkpointDirectory,
                                                      String outputPath) {

        System.out.println("Creating new context");
        File outputFile = new File(outputPath);
        if (outputFile.exists()) {
            outputFile.delete();
        }
        SparkConf sparkConf = new SparkConf().setAppName("Learn15CheckPoint").setMaster("local[2]");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        // 设置检查点
        ssc.checkpoint(checkpointDirectory);

        // 创建ReceiverInputStream
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(ip, port);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey(Integer::sum);

        wordCounts.foreachRDD((rdd, time) -> {
            // 创建广播变量
            Broadcast<List<String>> blacklist =
                    JavaWordBlacklist.getInstance(new JavaSparkContext(rdd.context()));
            // 创建累加器
            LongAccumulator droppedWordsCounter =
                    JavaDroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));
            // Use blacklist to drop words and use droppedWordsCounter to count them
            String counts = rdd.filter(wordCount -> {
                if (blacklist.value().contains(wordCount._1())) {
                    droppedWordsCounter.add(wordCount._2());
                    return false;
                } else {
                    return true;
                }
            }).collect().toString();
            String output = "Counts at time " + time + " " + counts;
            System.out.println(output);
            System.out.println("Dropped " + droppedWordsCounter.value() + " word(s) totally");
            System.out.println("Appending to " + outputFile.getAbsolutePath());
            Files.append(output + "\n", outputFile, Charset.defaultCharset());
        });

        return ssc;
    }

    public static void main(String[] args) throws Exception {

        String ip = "192.168.1.253";
        int port = 8020;
        String checkpointDirectory = "D:\\checkpoint";
        String outputPath = "E:\\output";

        // Function to create JavaStreamingContext without any output operations
        Function0<JavaStreamingContext> createContextFunc = () -> createContext(ip, port, checkpointDirectory, outputPath);

        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
        ssc.start();
        ssc.awaitTermination();
    }
}
