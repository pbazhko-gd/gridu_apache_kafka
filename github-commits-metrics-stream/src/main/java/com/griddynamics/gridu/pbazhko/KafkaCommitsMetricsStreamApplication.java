package com.griddynamics.gridu.pbazhko;

import com.griddynamics.gridu.pbazhko.config.AppConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class KafkaCommitsMetricsStreamApplication {

    public static void main(String[] args) {
        var context = new AnnotationConfigApplicationContext(AppConfig.class);
        context.registerShutdownHook();
    }
}
