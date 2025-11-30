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
    private final KafkaProducerHolder<String, GitHubAccount> kafkaProducerHolder;

    public void publish() {
        var accounts = gitHubAccountsReadingService.readAll();
        log.info("Found {} GitHub account(s)", accounts.size());
        accounts.forEach(this::publishAccount);
    }

    private void publishAccount(GitHubAccount account) {

        log.debug("Start processing account '{}'", account);

        var key = String.valueOf(account.getName().charAt(0));
        var producerRecord = new ProducerRecord<>(accountsTopic, key, account);

        kafkaProducerHolder.getKafkaProducer()
            .send(producerRecord, (data, error) -> {
                if (error == null) {
                    log.debug("Write account '{}' to partition: {}", account, data.partition());
                } else {
                    log.error("Cannot write account '{}' to Kafka", account, error);
                }
            });
    }
}
