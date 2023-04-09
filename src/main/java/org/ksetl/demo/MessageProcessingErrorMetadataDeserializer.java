package org.ksetl.demo;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class MessageProcessingErrorMetadataDeserializer extends ObjectMapperDeserializer<MessageProcessingErrorMetadata> {
    public MessageProcessingErrorMetadataDeserializer() {
        super(MessageProcessingErrorMetadata.class);
    }
}