package com.griddynamics.gridu.pbazhko;

import com.griddynamics.gridu.pbazhko.config.KafkaStreamsConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class KafkaCommitsMetricsStreamApplication {

    public static void main(String[] args) {
        var context = new AnnotationConfigApplicationContext(KafkaStreamsConfig.class);
        context.registerShutdownHook();
    }
}
