package com.griddynamics.gridu.pbazhko.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import com.griddynamics.gridu.pbazhko.model.metrics.CommitersMetricModel;
import com.griddynamics.gridu.pbazhko.model.metrics.LanguagesMetricModel;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.Map;
import java.util.Properties;

import static com.griddynamics.gridu.pbazhko.constants.Constants.*;
import static org.apache.kafka.streams.StreamsConfig.*;

@Slf4j
@Configuration
@RequiredArgsConstructor
@PropertySource("classpath:application.properties")
public class AppConfig {

    @Value("${KAFKA_BOOTSTRAP_SERVERS}")
    private String kafkaBootstrapServers;

    @Value("${KAFKA_SCHEMA_REGISTRY}")
    private String kafkaSchemaRegistry;

    @Value("${KAFKA_SCHEMA_AUTO_REGISTER}")
    private boolean kafkaSchemaAutoRegister;

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public KafkaJsonSchemaSerde<GitHubCommit> gitHubCommitKafkaJsonSchemaSerde() {
        return buildCustomSerde(GitHubCommit.class);
    }

    @Bean
    public KafkaJsonSchemaSerde<CommitersMetricModel> committersMetricModelKafkaJsonSchemaSerde() {
        return buildCustomSerde(CommitersMetricModel.class);
    }

    @Bean
    public KafkaJsonSchemaSerde<LanguagesMetricModel> languagesMetricModelKafkaJsonSchemaSerde() {
        return buildCustomSerde(LanguagesMetricModel.class);
    }

    @Bean("baseKafkaStreamProperties")
    public Properties baseKafkaStreamProperties() {
        var properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

        // Skip records with output topic write errors
        properties.put(DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, DefaultProductionExceptionHandler.class);

        // Skip records with deserialization errors
        properties.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        properties.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);

        return properties;
    }

    private <T> KafkaJsonSchemaSerde<T> buildCustomSerde(Class<T> clazz) {
        var serde = new KafkaJsonSchemaSerde<T>();
        serde.configure(Map.of(
            SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistry,
            AUTO_REGISTER_SCHEMAS_CONFIG, kafkaSchemaAutoRegister,
            JSON_VALUE_TYPE_CONFIG, clazz.getName()
        ), false);
        return serde;
    }
}
