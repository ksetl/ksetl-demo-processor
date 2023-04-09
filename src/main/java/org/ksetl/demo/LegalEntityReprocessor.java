package org.ksetl.demo;

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
import java.util.Optional;

@Path("/api/reprocess")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LegalEntityReprocessor {

    public static final Logger logger = LoggerFactory.getLogger(LegalEntityReprocessor.class);

    private final LegalEntityConsumer legalEntityConsumer;
    private final KafkaPicker kafkaPicker;

    public LegalEntityReprocessor(LegalEntityConsumer legalEntityConsumer, KafkaPicker kafkaPicker) {
        this.legalEntityConsumer = legalEntityConsumer;
        this.kafkaPicker = kafkaPicker;
    }

    @POST
    public Response post(MessageProcessingErrorMetadata messageProcessingErrorMetadata) {
        Optional<ConsumerRecord<String, LegalEntitySource>> optional = this.kafkaPicker.find(Constants.CHANNEL_LEGAL_ENTITY_IN, messageProcessingErrorMetadata.topic(), messageProcessingErrorMetadata.partition(), messageProcessingErrorMetadata.offset());
        if (optional.isPresent()) {
            legalEntityConsumer.process(optional.get());
            return Response.status(Response.Status.OK).build();
        } else {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
    }

}
