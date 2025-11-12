package com.griddynamics.gridu.pbazhko;

import com.griddynamics.gridu.pbazhko.factory.KafkaProducerFactory;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.service.GitHubAccountsProvidingService;
import com.griddynamics.gridu.pbazhko.service.GitHubAccountsPublishingService;
import com.griddynamics.gridu.pbazhko.util.EnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;

@Slf4j
public class GithubAccountsPublisherApplication {

    private static final String TOPIC =
        EnvUtil.getConfig("KAFKA_GITHUB_ACCOUNTS_TOPIC");

    private static final KafkaProducer<String, GitHubAccount> KAFKA_PRODUCER =
        KafkaProducerFactory.getProducer();

    private static final GitHubAccountsProvidingService GIT_HUB_ACCOUNTS_PROVIDING_SERVICE =
        new GitHubAccountsProvidingService();

    private static final GitHubAccountsPublishingService PUBLISHING_SERVICE =
        new GitHubAccountsPublishingService(KAFKA_PRODUCER, GIT_HUB_ACCOUNTS_PROVIDING_SERVICE);

    public static void main(String[] args) {
        PUBLISHING_SERVICE.publishToTopic(TOPIC);
    }
}
