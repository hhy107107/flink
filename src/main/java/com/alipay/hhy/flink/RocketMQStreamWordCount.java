package com.alipay.hhy.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.flink.legacy.RocketMQSink;
import org.apache.rocketmq.flink.legacy.RocketMQSource;
import org.apache.rocketmq.flink.legacy.common.selector.DefaultTopicSelector;
import org.apache.rocketmq.flink.legacy.common.serialization.SimpleKeyValueDeserializationSchema;
import org.apache.rocketmq.flink.legacy.common.serialization.SimpleKeyValueSerializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.rocketmq.flink.legacy.RocketMQConfig.*;

/**
 * <p>
 * description: 读取mq
 * 写入：https://www.jianshu.com/p/0d90074b5af6
 * 读取：https://github.com/apache/rocketmq-externals/tree/master/rocketmq-flink
 * @author hhy
 * <p>
 * create: 2021/6/2 12:15 下午
 **/
public class RocketMQStreamWordCount {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(NAME_SERVER_ADDR, "localhost:9876");
        consumerProps.setProperty(CONSUMER_GROUP, "c002");
        consumerProps.setProperty(CONSUMER_TOPIC, "flink-source2");

        Properties producerProps = new Properties();
        producerProps.setProperty(NAME_SERVER_ADDR, "localhost:9876");

        env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps))
                .name("rocketmq-source")
                .setParallelism(2)
                .process(new ProcessFunction<Map, Map>() {
                    @Override
                    public void processElement(Map in, Context ctx, Collector<Map> out) throws Exception {
                        HashMap result = new HashMap();
                        result.put("id", in.get("id"));
                        String[] arr = in.get("address").toString().split("\\s+");
                        result.put("province", arr[arr.length-1]);
                        out.collect(result);
                    }
                })
                .name("upper-processor")
                .setParallelism(2)
                .addSink(new RocketMQSink(producerProps).withBatchFlushOnCheckpoint(true))
                /*.addSink(new RocketMQSink(new SimpleKeyValueSerializationSchema("id", "province"),
                        new DefaultTopicSelector("flink-sink2"), producerProps).withBatchFlushOnCheckpoint(true))
               */ .name("rocketmq-sink")
                .setParallelism(2);

        try {
            env.execute("rocketmq-flink-example");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
