package com.griddynamics.gridu.pbazhko.config;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import java.util.Properties;

@Slf4j
@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackages = "com.griddynamics.gridu.pbazhko")
public class AppConfig {

    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String AUTO_REGISTER_SCHEMAS_CONFIG = "auto.register.schemas";

    @Bean(destroyMethod = "close")
    public KafkaProducer<String, GitHubCommit> kafkaProducer(Environment env) {
        return new KafkaProducer<>(getProducerProperties(env));
    }

    @Bean(destroyMethod = "close")
    public KafkaConsumer<String, GitHubAccount> kafkaConsumer(Environment env) {
        return new KafkaConsumer<>(getConsumerProperties(env));
    }

    private Properties getProducerProperties(Environment env) {
        var properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, env.getProperty("KAFKA_PRODUCER_CLIENT_ID"));
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, env.getProperty("KAFKA_ENABLE_IDEMPOTENCE"));
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("KAFKA_SCHEMA_REGISTRY"));
        properties.put(AUTO_REGISTER_SCHEMAS_CONFIG, env.getProperty("KAFKA_SCHEMA_AUTO_REGISTER"));
        return properties;
    }

    private static Properties getConsumerProperties(Environment env) {
        var properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, env.getProperty("KAFKA_CONSUMER_GROUP_ID"));
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, env.getProperty("KAFKA_CONSUMER_ENABLE_AUTOCOMMIT"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, env.getProperty("KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG"));
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("KAFKA_SCHEMA_REGISTRY"));
        return properties;
    }
}
