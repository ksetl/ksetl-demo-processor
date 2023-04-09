package org.ksetl.demo;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.restassured.http.ContentType;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
class LegalEntityProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(LegalEntityProcessorTest.class);

    @InjectKafkaCompanion
    KafkaCompanion kafkaCompanion;

    @Inject
    LegalEntityLookupService legalEntityLookupService;

    private final Serde<LegalEntitySource> legalEntitySourceSerde = new ObjectMapperSerde<>(LegalEntitySource.class);
    private final Serde<LegalEntityTarget> legalEntityTargetSerde = new ObjectMapperSerde<>(LegalEntityTarget.class);
    private final Serde<MessageProcessingErrorMetadata> messageProcessingErrorMetadataSerde = new ObjectMapperSerde<>(MessageProcessingErrorMetadata.class);

    @Test
    void successForFour() {

        LegalEntitySource legalEntitySource = new LegalEntitySource("TEST", "Test");
        kafkaCompanion.produce(Serdes.String(), legalEntitySourceSerde).fromRecords(new ProducerRecord<>(Constants.TOPIC_LEGAL_ENTITY, legalEntitySource.globalLegalEntityId(), legalEntitySource));
        ConsumerTask<Integer, LegalEntityTarget> consumerRecords = kafkaCompanion.consume(Serdes.Integer(), legalEntityTargetSerde)
                .fromTopics(Constants.TOPIC_LEGAL_ENTITY_ETRM1).awaitRecords(1);
        List<LegalEntityTarget> legalEntityTargets = consumerRecords.stream()
                .peek(record -> logger.info("{}", record.value()))
                .map(ConsumerRecord::value)
                .toList();
        assertThat(legalEntityTargets).contains(new LegalEntityTarget(4, "Test"));
    }

    @Test
    void failForSix() {
        LegalEntitySource legalEntitySource = new LegalEntitySource("SIXSIX", "Fail");
        kafkaCompanion.produce(Serdes.String(), legalEntitySourceSerde).fromRecords(new ProducerRecord<>(Constants.TOPIC_LEGAL_ENTITY, legalEntitySource.globalLegalEntityId(), legalEntitySource));
        ConsumerTask<String, MessageProcessingErrorMetadata> consumerRecords = kafkaCompanion.consume(Serdes.String(), messageProcessingErrorMetadataSerde)
                .fromTopics(Constants.TOPIC_MESSAGE_PROCESSING_ERROR_METADATA).awaitRecords(1, Duration.ofSeconds(100));
        legalEntityLookupService.setAllPass(true);
        given()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .body(consumerRecords.stream().findFirst().get().value())
                .post("/api/reprocess")
                .then()
                .statusCode(200);
        ConsumerTask<Integer, LegalEntityTarget> targetConsumerTask = kafkaCompanion.consume(Serdes.Integer(), legalEntityTargetSerde)
                .fromTopics(Constants.TOPIC_LEGAL_ENTITY_ETRM1).awaitRecords(1, Duration.ofSeconds(100));
        List<LegalEntityTarget> legalEntityTargets = targetConsumerTask.stream()
                .peek(record -> logger.info("{}", record.value()))
                .map(ConsumerRecord::value)
                .toList();
        assertThat(legalEntityTargets).contains(new LegalEntityTarget(6, "Fail"));
    }

}