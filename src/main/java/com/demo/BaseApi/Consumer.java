package com.demo.BaseApi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;


/*******************************************************************************
 * @Description: Consumer 消费者 Kafka consumer不是线程安全的
 ******************************************************************************/
public class Consumer implements Runnable{

    public static String TOPIC = "my-output-topic";
    public  static String KAFKA_ADDRESS="192.168.0.133:9092";
    public  static String ZOOKEEPER_ADDRESS="192.168.0.133:2181";

    public static void main(String[] args) throws IOException {
        runConsumer();
    }

    public static void runConsumer() {
        String group = "0";

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_ADDRESS);
        props.put("zookeeper.connect", ZOOKEEPER_ADDRESS);
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("Subscribed to topic " + TOPIC);
        Long unixTime;
        Long totalLatency = 0L;
        Long count = 0L;
        Long minCreationTime = Long.MAX_VALUE;

        while (true) {
            System.out.println("start consumer..."+new Date());
            // 如果没有数据就等待100ms。如果有就读取。
            ConsumerRecords<String, Long> records = consumer.poll(100);
            // 迭代每一个partition
            for (TopicPartition partition : records.partitions()) {

                // 每一个partition的数据
                List<ConsumerRecord<String, Long>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, Long> record : partitionRecords) {
                    // For benchmarking tests
                    Long ts = record.timestamp();
                    if (ts < minCreationTime) {
                        minCreationTime = ts;
                    }
                    //TimestampType tp = record.timestampType();
                    unixTime = System.currentTimeMillis();
                    Long latency = unixTime - ts;
                    totalLatency += latency;
                    count += 1;
                    System.out.println(getNowDate() + " partition:" + record.partition() + " region(key): " + record.key() + "  clicks(value): " + record.value() + "   outputTime: " + unixTime + " minCreationTime : " + minCreationTime + "  totallatency: " + totalLatency + "  count: " + count + " offset" + record.offset());
                }

            }

        }
    }

    /**
     * 获取现在时间
     *
     * @return 返回时间类型 yyyy-MM-dd HH:mm:ss
     */
    public static String getNowDate() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    @Override
    public void run() {
        runConsumer();
    }
}