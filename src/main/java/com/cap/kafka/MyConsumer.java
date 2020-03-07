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

    private Consumer<Long, String> consumer;
    private final Properties props = new Properties();

    public MyConsumer(String group) {

        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    public MyConsumer bootstrap(String server) {

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        this.consumer = new KafkaConsumer<>(props);
        return this;
    }

    public MyConsumer subscribe(String topic) {
        this.consumer.subscribe(Collections.singletonList(topic));
        return this;
    }

    private void printTopics(){
        //print subscribed topics
        Set<String> topics = consumer.listTopics().keySet();
        topics.forEach(topic->
                System.out.println("My topic is : " + topic)
        );
    }

    private void printPartitions(){
        //print assigned partitions
        Set<TopicPartition> partitions = consumer.assignment();
        partitions.forEach(
                part->System.out.println("Partition " + part.partition() + "assigned")
        );
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
}
