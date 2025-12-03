package com.griddynamics.gridu.pbazhko.config;

import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import java.util.Map;

import static com.griddynamics.gridu.pbazhko.constants.Constants.*;

@Slf4j
@Configuration
@PropertySource("classpath:application.properties")
@RequiredArgsConstructor
@ComponentScan(basePackages = "com.griddynamics.gridu.pbazhko")
public class AppConfig {

    private final Environment env;

    @Bean
    public KafkaJsonSchemaSerde<GitHubCommit> gitHubCommitKafkaJsonSchemaSerde() {
        var serde = new KafkaJsonSchemaSerde<GitHubCommit>();
        serde.configure(Map.of(
            SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("KAFKA_SCHEMA_REGISTRY"),
            AUTO_REGISTER_SCHEMAS_CONFIG, env.getProperty("KAFKA_SCHEMA_AUTO_REGISTER"),
            JSON_VALUE_TYPE_CONFIG, GitHubCommit.class.getName()
        ), false);
        return serde;
    }
}
