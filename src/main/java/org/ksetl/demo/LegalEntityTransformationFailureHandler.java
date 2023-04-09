package org.ksetl.demo;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class LegalEntityTransformationFailureHandler {

    private final String kafkaBootstrapServers;
    private final Emitter<MessageProcessingErrorMetadata> emitter;

    public LegalEntityTransformationFailureHandler(
            @ConfigProperty(name = "kafka.bootstrap.servers") String kafkaBootstrapServers,
            @Channel("message-processing-error-metadata-out") Emitter<MessageProcessingErrorMetadata> emitter) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.emitter = emitter;
    }

    public void handle(ConsumerRecord<String, LegalEntitySource> source) {
        String topic = source.topic();
        int partition = source.partition();
        long offset = source.offset();
        Log.infov("kafka.bootstrap.servers[{0}], topic[{1}], partition[{2}], offset[{3}], k[{4}]: {5}", kafkaBootstrapServers, topic, partition, offset, source.key(), source.value());
        var messageProcessingErrorMetadata = new MessageProcessingErrorMetadata(kafkaBootstrapServers, topic, partition, offset);
        emitter.send(Message.of(messageProcessingErrorMetadata).addMetadata(OutgoingKafkaRecordMetadata.builder().withKey(topic)));
    }
}
