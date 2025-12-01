package com.griddynamics.gridu.pbazhko.config;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackages = "com.griddynamics.gridu.pbazhko")
public class AppConfig {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    public static final String AUTO_REGISTER_SCHEMAS_CONFIG = "auto.register.schemas";

    @Bean
    public KafkaProducerHolder<String, GitHubAccount> kafkaProducerHolder(Environment env) {
        var kafkaProducer = new KafkaProducer<String, GitHubAccount>(getProperties(env));
        return new KafkaProducerHolder<>(kafkaProducer);
    }

    private static Properties getProperties(Environment env) {
        var properties = new Properties();
        properties.put(CLIENT_ID_CONFIG, env.getProperty("KAFKA_PRODUCER_CLIENT_ID"));
        properties.put(BOOTSTRAP_SERVERS_CONFIG, env.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        properties.put(ENABLE_IDEMPOTENCE_CONFIG, env.getProperty("KAFKA_ENABLE_IDEMPOTENCE"));
        properties.put(ACKS_CONFIG, env.getProperty("KAFKA_PRODUCER_ACKS"));
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("KAFKA_SCHEMA_REGISTRY"));
        properties.put(AUTO_REGISTER_SCHEMAS_CONFIG, env.getProperty("KAFKA_SCHEMA_AUTO_REGISTER"));
        return properties;
    }
}
