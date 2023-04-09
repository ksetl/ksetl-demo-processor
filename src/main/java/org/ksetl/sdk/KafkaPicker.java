package org.ksetl.sdk;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.Optional;

@ApplicationScoped
public class KafkaPicker {

    @Inject
    KafkaClientService kafkaClientService;

    public <K, V> Optional<ConsumerRecord<K, V>> find(String channel, String topic, int partition, long offset) {
        KafkaConsumer<K, V> kafkaConsumer = kafkaClientService.getConsumer(channel);
        return kafkaConsumer.runOnPollingThread(consumer -> {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.seek(topicPartition, offset);
            ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() > 1) {
                Log.warnv("ConsumerRecords.count() is {0}, but should be 1", consumerRecords.count());
            }
            return consumerRecords.iterator().next();
        }).await().asOptional().atMost(Duration.ofSeconds(5));
    }

}
