package com.griddynamics.gridu.pbazhko;

import com.google.inject.Guice;
import com.griddynamics.gridu.pbazhko.config.AppModule;
import com.griddynamics.gridu.pbazhko.service.GitHubAccountsPublishingService;
import com.griddynamics.gridu.pbazhko.util.EnvUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GithubAccountsPublisherApplication {

    private static final String TOPIC = EnvUtil.getConfig("KAFKA_GITHUB_ACCOUNTS_TOPIC");

    public static void main(String[] args) {
        var injector = Guice.createInjector(new AppModule());
        var publishingService = injector.getInstance(GitHubAccountsPublishingService.class);

        publishingService.publishToTopic(TOPIC);
    }
}
