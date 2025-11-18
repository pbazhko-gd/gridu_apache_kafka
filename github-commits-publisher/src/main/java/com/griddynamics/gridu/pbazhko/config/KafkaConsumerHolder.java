package com.griddynamics.gridu.pbazhko.config;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerHolder implements AutoCloseable {

    @Getter
    private final KafkaConsumer<String, GitHubAccount> kafkaConsumer;

    @Override
    public void close() {
        log.info("Close Kafka kafkaConsumer");
        kafkaConsumer.close();
    }
}
