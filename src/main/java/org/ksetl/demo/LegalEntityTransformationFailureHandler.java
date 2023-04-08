package org.ksetl.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class LegalEntityTransformationFailureHandler {
    public void handle(ConsumerRecord<String, LegalEntitySource> source) {
    }
}
