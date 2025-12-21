package com.griddynamics.gridu.pbazhko.stream;

import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import com.griddynamics.gridu.pbazhko.model.CommitersMetricModel;
import com.griddynamics.gridu.pbazhko.model.LanguagesMetricModel;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.Map;

import static com.griddynamics.gridu.pbazhko.constants.Constants.JSON_VALUE_TYPE_CONFIG;
import static com.griddynamics.gridu.pbazhko.constants.Constants.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
public class TestConfig {

    @Bean
    @Primary
    public KafkaJsonSchemaSerde<GitHubCommit> gitHubCommitKafkaJsonSchemaSerde() {
        return buildCustomJsonSchemaSerde(GitHubCommit.class);
    }

    @Bean
    @Primary
    public KafkaJsonSchemaSerde<CommitersMetricModel> committersMetricModelKafkaJsonSchemaSerde() {
        return buildCustomJsonSchemaSerde(CommitersMetricModel.class);
    }

    @Bean
    @Primary
    public KafkaJsonSchemaSerde<LanguagesMetricModel> languagesMetricModelKafkaJsonSchemaSerde() {
        return buildCustomJsonSchemaSerde(LanguagesMetricModel.class);
    }

    private <T> KafkaJsonSchemaSerde<T> buildCustomJsonSchemaSerde(Class<T> clazz) {
        var mockSchemaRegistryClient = new MockSchemaRegistryClient();
        var serde = new KafkaJsonSchemaSerde<T>(mockSchemaRegistryClient);
        serde.configure(Map.of(
            SCHEMA_REGISTRY_URL_CONFIG, "mock://sr",
            JSON_VALUE_TYPE_CONFIG, clazz.getName()
        ), false);
        return serde;
    }
}
