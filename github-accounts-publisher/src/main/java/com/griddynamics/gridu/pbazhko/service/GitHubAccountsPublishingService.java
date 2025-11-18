package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.config.KafkaProducerHolder;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class GitHubAccountsPublishingService {

    @Value("${KAFKA_GITHUB_ACCOUNTS_TOPIC}")
    private String accountsTopic;

    private final GitHubAccountsReadingService gitHubAccountsReadingService;
    private final KafkaProducerHolder kafkaProducerHolder;

    public void publish() {
        gitHubAccountsReadingService.readAll()
            .forEach(this::publishAccount);
        kafkaProducerHolder.getKafkaProducer().flush();
    }

    private void publishAccount(GitHubAccount account) {
        var key = String.valueOf(account.getName().charAt(0));
        var record = new ProducerRecord<>(accountsTopic, key, account);
        kafkaProducerHolder.getKafkaProducer()
            .send(record, (data, error) -> {
                if (error == null) {
                    log.info("Write to partition: {}", data.partition());
                } else {
                    log.error("Cannot write to Kafka", error);
                }
            });
    }
}
