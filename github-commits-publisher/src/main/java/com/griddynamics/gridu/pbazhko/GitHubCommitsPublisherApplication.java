package com.griddynamics.gridu.pbazhko;

import com.griddynamics.gridu.pbazhko.config.AppConfig;
import com.griddynamics.gridu.pbazhko.service.GitHubAccountsProcessingService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class GitHubCommitsPublisherApplication {

    public static void main(String[] args) {
        var context = new AnnotationConfigApplicationContext(AppConfig.class);
        var service = context.getBean(GitHubAccountsProcessingService.class);
        service.process();
    }
}
