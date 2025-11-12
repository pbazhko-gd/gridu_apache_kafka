package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
@RequiredArgsConstructor
public class GitHubAccountsPublishingService {

    private final KafkaProducer<String, GitHubAccount> kafkaProducer;
    private final GitHubAccountsProvidingService accountsProvidingService;

    public void publishToTopic(String topic) {
        accountsProvidingService.findAll()
            .forEach(account -> publishAccount(topic, account));
    }

    @SneakyThrows
    private void publishAccount(String topic, GitHubAccount account) {
        var key = String.valueOf(account.getName().charAt(0));
        var record = new ProducerRecord<String, GitHubAccount>(topic, key, account);
        kafkaProducer.send(record, (data, error) -> {
            if (error == null) {
                log.info("Write to partition: {}", data.partition());
            } else {
                log.error("Cannot write to Kafka", error);
            }
        });
    }
}
