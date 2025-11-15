package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class GitHubAccountsProcessingService {

    @Value("${KAFKA_GITHUB_ACCOUNTS_TOPIC}")
    private final String accountsTopic;

    @Value("${KAFKA_GITHUB_COMMITS_TOPIC}")
    private final String commitsTopic;

    private final KafkaConsumer<String, GitHubAccount> kafkaConsumer;
    private final KafkaProducer<String, GitHubCommit> kafkaProducer;

    public void process() {

    }
}
