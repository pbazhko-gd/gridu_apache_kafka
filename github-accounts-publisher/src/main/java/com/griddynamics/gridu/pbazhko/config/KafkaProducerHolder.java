package com.griddynamics.gridu.pbazhko.config;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducerHolder implements AutoCloseable {

    @Getter
    private final KafkaProducer<String, GitHubAccount> kafkaProducer;

    @Override
    public void close() {
        log.info("Flush Kafka producer");
        kafkaProducer.flush();

        log.info("Close Kafka producer");
        kafkaProducer.close();
    }
}
