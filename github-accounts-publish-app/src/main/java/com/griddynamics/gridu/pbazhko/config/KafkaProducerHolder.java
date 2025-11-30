package com.griddynamics.gridu.pbazhko.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducerHolder<K, V> implements AutoCloseable {

    @Getter
    private final KafkaProducer<K, V> kafkaProducer;

    @Override
    public void close() {
        log.info("Flush Kafka producer");
        kafkaProducer.flush();

        log.info("Close Kafka producer");
        kafkaProducer.close();
    }
}
