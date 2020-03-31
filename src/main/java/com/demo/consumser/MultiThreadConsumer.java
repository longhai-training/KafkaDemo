package com.demo.consumser;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/*******************************************************************************
 * @Description: 多线程消费实例
 ******************************************************************************/
public enum MultiThreadConsumer {

    //kafka Consumer实例
    instance;

    private KafkaConsumer<String, String> consumer;
    private String topic;
    // Threadpool of consumers
    private ExecutorService executor;

    private AtomicBoolean isShutdown;
    private ConsumerRecords<String, String> records;
    static int index = 0;
    int count_index = 0;

    public void init(String brokers, String groupId, String topic) {
        isShutdown = new AtomicBoolean(false);

        Properties properties = buildKafkaProperty(brokers, groupId);
        this.consumer = new KafkaConsumer<>(properties);
        this.topic = topic;

        this.consumer.subscribe(Arrays.asList(this.topic), consumerRebalanceListener); // 订阅主题
        System.out.println("Subscribed to topic " + topic);

    }

    public void start(int threadNumber, CountDownLatch countDownLatch1) {
        Set<TopicPartition> partitions = consumer.assignment();
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
            long lastOffset = offsetAndMetadata.offset();
            if (consumer != null) {
                System.out.println("rebalance to partition:" + partition + " offset:" + lastOffset);
                consumer.seek(partition, lastOffset); // 指定当前partition消费的位置。  Specify the location of the current partition consumption.
            }

        }

        executor = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        try {

            while (!isShutdown.get()) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        Long unixTime;
                        Long totalLatency = 0L;
                        Long count = 0L;
                        Long minCreationTime = Long.MAX_VALUE;


                        if (consumer != null) {
                            records = consumer.poll(100);
                        }

                        // TODO 模拟程序退出
                        System.out.println("已消费:" + index + "次!");
                        if (index == 6) {
                            System.out.println("终止程序!");
                            System.exit(0);
                        }
                        index++;

                        if (records != null && !records.isEmpty()) {

                            // 迭代每一个partition
                            for (TopicPartition partition : records.partitions()) {

                                // 每一个partition的数据
                                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                                for (ConsumerRecord<String, String> record : partitionRecords) {
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

                                    System.out.println(getNowDate() + " thread:" + Thread.currentThread().getName() + " partition:" + record.partition() + " region(key): " + record.key() + "  clicks(value): " + record.value() + "   outputTime: " + unixTime + " minCreationTime : " + minCreationTime + "  totallatency: " + totalLatency + "  count: " + count + " offset" + record.offset());
                                    // poll 消费每一条数据后,自动提交offset到当前的partition。  After each data is consumed, offset is automatically submitted to the current partition.
                                    long offset = record.offset(); // 当前已经消费过的offset。  Offset, which is currently consumed。
                                    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = Collections.singletonMap(
                                            partition, new OffsetAndMetadata(offset + 1)); // 由于手动提交,offset需要+1,指向下一个还没有消费的offset。 Due to manual submission, offset needs +1, pointing to the next offset that has not been consumed yet.

                                    CustomMessage message = new CustomMessage(record.partition() + "00000000" + record.offset(), record.value(), offsetAndMetadataMap, partition);
                                    try {
                                        ConsumerThreadMain.jobQueue.put(message); // 放入队列中
                                        doCommit(offsetAndMetadataMap);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    } catch (ConcurrentModificationException e) {
                                        System.out.println("ConcurrentModificationException!!!");
                                    }

                                }

                            }
                            records = null;
                            // 使用完poll从本地缓存拉取到数据之后,需要client调用commitSync方法（或者commitAsync方法）去commit 下一次该去读取 哪一个offset的message。
                            // consumer.commitSync();
                        }
                    }
                });
            }

            if (records != null && !records.isEmpty()) {
                System.out.println("records.count():" + records.count());
                Long unixTime;
                Long totalLatency = 0L;
                Long count = 0L;
                Long minCreationTime = Long.MAX_VALUE;
                // 继续消费
                // 迭代每一个partition
                for (TopicPartition partition : records.partitions()) {

                    // 每一个partition的数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
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

                        try {
                            System.out.println("last consumer ! ---" + getNowDate() + " thread:" + Thread.currentThread().getName() + " partition:" + record.partition() + " region(key): " + record.key() + "  clicks(value): " + record.value() + "   outputTime: " + unixTime + " minCreationTime : " + minCreationTime + "  totallatency: " + totalLatency + "  count: " + count + " offset" + record.offset() + " count_index:" + count_index);
                            // poll 消费每一条数据后,自动提交offset到当前的partition。  After each data is consumed, offset is automatically submitted to the current partition.
                            long offset = record.offset(); // 当前已经消费过的offset。  Offset, which is currently consumed。
                            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = Collections.singletonMap(
                                    partition, new OffsetAndMetadata(offset + 1)); // 由于手动提交,offset需要+1,指向下一个还没有消费的offset。 Due to manual submission, offset needs +1, pointing to the next offset that has not been consumed yet.

                            CustomMessage message = new CustomMessage(record.partition() + "00000000" + record.offset(), record.value(), offsetAndMetadataMap, partition);

                            ConsumerThreadMain.jobQueue.put(message); // 放入队列中
                            consumer.commitSync(offsetAndMetadataMap);
                            count_index++;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
            }

            countDownLatch1.countDown(); // 解锁

        } catch (WakeupException e) {
            System.out.println("Catch WakeupException ! Start Consumer Close ...");
            if (consumer != null && isShutdown.get()) {
                consumer.close();
            }
        } finally {
            System.out.println("finally!");
            if (consumer != null) {
                consumer.close();
            }
        }

    }

    private static Properties buildKafkaProperty(String brokers, String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
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

    public void stop() throws InterruptedException {
        System.out.println("start stop commond !");
        if (isShutdown != null) {
            isShutdown.set(true);
        }
    }

    private ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {

        // 保存偏移量 保存每一个partition已经提交消费的offset。 Save the offset to save offset for each partition that has already been submitted to the consumption.
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            //consumer.commitSync(); // 保存offset
        }

        // 提取偏移量 Extraction of offset
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println(" onPartitionsAssigned partitions.size:" + partitions.size());
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
                long lastOffset = offsetAndMetadata.offset();
                if (consumer != null) {
                    System.out.println("rebalance to partition:" + partition + " offset:" + lastOffset);
                    consumer.seek(partition, lastOffset); // 指定当前partition消费的位置。  Specify the location of the current partition consumption.
                }

            }
        }
    };

    /**
     * 提交offset
     */
    public void doCommit(Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap) {
        try {
            consumer.commitSync(offsetAndMetadataMap);
        } catch (Exception e) {
            doCommit(offsetAndMetadataMap);
        }
    }

    /**
     * 自定义消息实体对象
     */
    class CustomMessage implements Serializable {
        String v1;
        String v2;
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap;
        TopicPartition partition;

        public CustomMessage(String v1, String v2, Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap, TopicPartition partition) {
            this.v1 = v1;
            this.v2 = v2;
            this.offsetAndMetadataMap = offsetAndMetadataMap;
            this.partition = partition;
        }
    }
}