package com.griddynamics.gridu.pbazhko;

import com.griddynamics.gridu.pbazhko.config.AppConfig;
import com.griddynamics.gridu.pbazhko.service.GitHubAccountsPublishingService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class GitHubAccountsPublisherApplication {

    public static void main(String[] args) {
        var context = new AnnotationConfigApplicationContext(AppConfig.class);
        try (context) {
            var service = context.getBean(GitHubAccountsPublishingService.class);
            service.publish();
        }
    }
}
