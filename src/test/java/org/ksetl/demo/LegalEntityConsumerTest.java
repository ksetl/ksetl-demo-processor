package org.ksetl.demo;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
class LegalEntityConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(LegalEntityConsumerTest.class);

    @InjectKafkaCompanion
    KafkaCompanion kafkaCompanion;

    @Test
    void test() {
        Serde<LegalEntitySource> legalEntitySourceSerde = new ObjectMapperSerde<>(LegalEntitySource.class);
        Serde<LegalEntityTarget> legalEntityTargetSerde = new ObjectMapperSerde<>(LegalEntityTarget.class);
        LegalEntitySource legalEntitySource = new LegalEntitySource("TEST", "Test");
        kafkaCompanion.produce(Serdes.String(), legalEntitySourceSerde).fromRecords(new ProducerRecord<>("legal-entity", legalEntitySource.globalLegalEntityId(), legalEntitySource));

        ConsumerTask<Integer, LegalEntityTarget> consumerRecords = kafkaCompanion.consume(Serdes.Integer(), legalEntityTargetSerde)
                .fromTopics("legal-entity-etrm1").awaitRecords(1);
        List<LegalEntityTarget> legalEntityTargets = consumerRecords.stream()
                .map(ConsumerRecord::value)
                .toList();
        assertThat(legalEntityTargets).contains(new LegalEntityTarget(4, "Test"));
    }
}