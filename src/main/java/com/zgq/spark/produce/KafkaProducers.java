package com.zgq.spark.produce;

import com.alibaba.fastjson.JSON;
import com.zgq.spark.model.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka 生产者
 *
 * @classname KafkaProducers
 * @date 2019年09月20日 9:39
 */
public class KafkaProducers {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        // kafka集群节点
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cn75.hadoop.hisign:9092,cn76.hadoop.hisign:9092,cn77.hadoop.hisign:9092");
        // 关键字的序列化类。如果没给与这项，默认情况是和消息一致
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // producer需要server接收到数据之后发出的确认接收的信号
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // 设置大于0的值将使客户端重新发送任何数据，一旦这些数据发送失败。
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        // 配置控制默认的批量处理消息字节数。
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16385);
        // 延时配置
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // 定义topic
        String topic = "kafkaStreaming";

        // 循环发送100个人员信息
        for (int i = 0; i < 100; i++) {
            Person person = new Person("name" + i, 25);
            String value = JSON.toJSONString(person);
            kafkaProducer.send(new ProducerRecord<>(topic, "person", value));
            System.out.println(value);
        }

        kafkaProducer.close();
    }
}
