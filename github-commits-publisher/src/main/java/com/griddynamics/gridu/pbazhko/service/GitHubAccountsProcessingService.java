package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.config.KafkaConsumerHolder;
import com.griddynamics.gridu.pbazhko.config.KafkaProducerHolder;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@Service
@RequiredArgsConstructor
public class GitHubAccountsProcessingService {

    @Value("${KAFKA_GITHUB_ACCOUNTS_TOPIC}")
    private String accountsTopic;

    @Value("${KAFKA_GITHUB_COMMITS_TOPIC}")
    private String commitsTopic;

    @Value("${CONSUMER_POLLING_TIMEOUT_MS}")
    private Integer consumerPollingTimeout;

    private final KafkaConsumerHolder kafkaConsumerHolder;
    private final KafkaProducerHolder kafkaProducerHolder;
    private final GitHubCommitsRetrievingService commitsRetrievingService;

    public void process() {
        kafkaConsumerHolder.getKafkaConsumer().subscribe(Collections.singletonList(accountsTopic));
        getAccounts()
            .flatMap(commitsRetrievingService::getCommits)
            .doOnNext(this::publishCommit)
            .onErrorContinue((ex, ee) -> {
                log.error("Error", ex);
            })
            .subscribe();
        kafkaProducerHolder.getKafkaProducer().flush();
    }

    private Flux<GitHubAccount> getAccounts() {
        return Flux.fromIterable(kafkaConsumerHolder.getKafkaConsumer().poll(Duration.ofMillis(consumerPollingTimeout)))
            .doOnNext(r -> log.info("Partition: {}, Offset:{}", r.partition(), r.offset()))
            .doOnNext(r -> log.info("Key: {}, Value: {}", r.key(), r.value()))
            .map(ConsumerRecord::value);
    }

    private void publishCommit(GitHubCommit gitHubCommit) {
        log.info("Prepare commit to publish to Kafka '{}'", gitHubCommit);
        var record = new ProducerRecord<>(commitsTopic, gitHubCommit.getSha(), gitHubCommit);
        kafkaProducerHolder.getKafkaProducer().send(record);
    }
}
