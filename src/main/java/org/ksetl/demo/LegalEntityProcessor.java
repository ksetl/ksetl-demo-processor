package org.ksetl.demo;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
public class LegalEntityProcessor {

    private final String targetSystemId;
    private final LegalEntityLookupService legalEntityLookupService;
    private final LegalEntityTransformationFailureHandler legalEntityTransformationFailureHandler;
    private final Emitter<LegalEntityTarget> emitter;

    public LegalEntityProcessor(
            @ConfigProperty(name = "targetSystemId") String targetSystemId,
            LegalEntityLookupService legalEntityLookupService,
            LegalEntityTransformationFailureHandler legalEntityTransformationFailureHandler,
            @Channel("legal-entity-out") Emitter<LegalEntityTarget> emitter) {
        this.targetSystemId = targetSystemId;
        this.legalEntityLookupService = legalEntityLookupService;
        this.legalEntityTransformationFailureHandler = legalEntityTransformationFailureHandler;
        this.emitter = emitter;
    }

    @Incoming("legal-entity-in")
    public void process(ConsumerRecord<String, LegalEntitySource> source) {
        Log.infov("Start Processing: {0}, {1}", source.key(), source.value());
        LegalEntitySource legalEntitySource = source.value();
        Optional<Integer> legalEntityId = legalEntityLookupService.findLegalEntityId(legalEntitySource.globalLegalEntityId(), targetSystemId);
        if (legalEntityId.isPresent()) {
            LegalEntityTarget legalEntityTarget = new LegalEntityTarget(legalEntityId.get(), legalEntitySource.legalEntityName());
            emitter.send(Message.of(legalEntityTarget).addMetadata(OutgoingKafkaRecordMetadata.<Integer>builder()
                    .withKey(legalEntityTarget.legalEntityId())
                    .build()));
        } else {
            legalEntityTransformationFailureHandler.handle(source);
        }
    }

}
