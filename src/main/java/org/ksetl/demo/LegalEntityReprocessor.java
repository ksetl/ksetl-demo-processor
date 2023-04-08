package org.ksetl.demo;

import io.smallrye.common.annotation.Identifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.ksetl.sdk.KafkaPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("/api/reprocess")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LegalEntityReprocessor {

    public static final Logger logger = LoggerFactory.getLogger(LegalEntityReprocessor.class);

    private final Map<String, Object> configs;
    private final LegalEntityConsumer legalEntityConsumer;
    private final KafkaPicker kafkaPicker;

    public LegalEntityReprocessor(@Identifier("default-kafka-broker") Map<String, Object> configs, LegalEntityConsumer legalEntityConsumer) {
        this.configs = configs;
        this.configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.configs.put("value.deserializer", "org.ksetl.demo.LegalEntitySourceDeserializer");
        this.legalEntityConsumer = legalEntityConsumer;
        this.kafkaPicker = new KafkaPicker(configs);
    }

    @POST
    public Response post(MessageProcessingErrorMetadata messageProcessingErrorMetadata) {
        ConsumerRecord<String, LegalEntitySource> picked = kafkaPicker.pick(messageProcessingErrorMetadata.topic(), messageProcessingErrorMetadata.partition(), messageProcessingErrorMetadata.offset());
        legalEntityConsumer.process(picked);
        return Response.status(Response.Status.OK).build();
    }

}
