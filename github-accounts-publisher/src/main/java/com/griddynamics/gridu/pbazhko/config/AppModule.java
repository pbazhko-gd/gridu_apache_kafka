package com.griddynamics.gridu.pbazhko.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.griddynamics.gridu.pbazhko.factory.KafkaProducerFactory;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import org.apache.kafka.clients.producer.KafkaProducer;

public class AppModule extends AbstractModule {

    @Provides
    public KafkaProducer<String, GitHubAccount> kafkaProducer() {
        return KafkaProducerFactory.getProducer();
    }
}
