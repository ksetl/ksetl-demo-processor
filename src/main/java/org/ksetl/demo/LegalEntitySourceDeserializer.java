package org.ksetl.demo;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class LegalEntitySourceDeserializer extends ObjectMapperDeserializer<LegalEntitySource> {
    public LegalEntitySourceDeserializer() {
        super(LegalEntitySource.class);
    }
}