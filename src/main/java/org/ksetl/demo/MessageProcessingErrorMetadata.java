package org.ksetl.demo;

public record MessageProcessingErrorMetadata(String brokerAddress, String topic, int partition, long offset) {
}
