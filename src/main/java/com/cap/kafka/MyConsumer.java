package com.cap.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class MyConsumer {

    private static final MyConsumer instance = new MyConsumer();

    private static Consumer<Long, String> consumer;
    private static final Properties props = new Properties();
    private static int partitionsCount = 0;

    private MyConsumer(){}

    public static MyConsumer getInstance(){
        return instance;
    }

    public MyConsumer configure(Boolean autoCommit, String resetOffset){
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetOffset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        return this;
    }

    public MyConsumer configure(Boolean autoCommit){
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        return this;
    }

    public MyConsumer configure(){
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        return this;
    }

    public MyConsumer join(String group) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        return this;
    }

    public MyConsumer bootstrap(String server) {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        return this;
    }

    public MyConsumer start(){
        consumer = new KafkaConsumer<>(props);
        return this;
    }

    public MyConsumer subscribeTo(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
        return this;
    }


    public void poll(Duration period) {

        this.printTopics();

        while (true) {

            this.printPartitions();

            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(period);

            for (ConsumerRecord<Long, String> record: consumerRecords) {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            }
            //consumer.commitAsync();
        }
    }

    private void printTopics(){
        //print subscribed topics
        Set<String> topics = consumer.listTopics().keySet();
        topics.forEach(topic-> {
            if (!topic.equals("__consumer_offsets"))
                System.out.println("Im consuming topic : " + topic + '\n');
            }
        );
    }

    private void printPartitions(){
        //print assigned partitions when change
        Set<TopicPartition> partitions = consumer.assignment();
        if (partitionsCount != partitions.size()) {
            partitions.forEach(partition->
                    System.out.println("Partition " + partition.partition() + " assigned")
            );
            System.out.print('\n');
            partitionsCount = partitions.size();
        }
    }
}
