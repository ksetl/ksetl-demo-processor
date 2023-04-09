package org.ksetl.sdk;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public class KafkaPicker {

    private final Map<String, Object> configs;

    public KafkaPicker(Map<String, Object> configs) {
        configs.put("max.poll.records", "1");
        this.configs = configs;
    }

    public <K, V> ConsumerRecord<K, V> pick(String topic, int partition, long offset, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        KafkaConsumer<K, V> consumer = new KafkaConsumer(configs, keyDeserializer, valueDeserializer);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, offset);
        ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(5));
        ConsumerRecord<K, V> record = records.iterator().next();
        consumer.close();
        return record;
    }
}
