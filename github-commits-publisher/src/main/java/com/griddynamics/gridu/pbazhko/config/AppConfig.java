package com.griddynamics.gridu.pbazhko.config;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import reactor.netty.http.client.HttpClient;

import java.util.Properties;

@Slf4j
@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackages = "com.griddynamics.gridu.pbazhko")
public class AppConfig {

    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String AUTO_REGISTER_SCHEMAS_CONFIG = "auto.register.schemas";
    private static final String JSON_VALUE_TYPE_CONFIG = "json.value.type";

    @Bean
    public KafkaConsumerHolder kafkaConsumerHolder(Environment env) {
        var kafkaConsumer = new KafkaConsumer<String, GitHubAccount>(getConsumerProperties(env));
        return new KafkaConsumerHolder(kafkaConsumer);
    }

    @Bean
    public KafkaProducerHolder kafkaProducerHolder(Environment env) {
        var kafkaProducer = new KafkaProducer<String, GitHubCommit>(getProducerProperties(env));
        return new KafkaProducerHolder(kafkaProducer);
    }

    @Bean
    public HttpClient gitHubHttpClient() {
        return HttpClient.create()
            .compress(true)
            .keepAlive(true);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
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
        properties.put(JSON_VALUE_TYPE_CONFIG, GitHubAccount.class.getName());
        return properties;
    }

    private Properties getProducerProperties(Environment env) {
        var properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, env.getProperty("KAFKA_PRODUCER_CLIENT_ID"));
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, env.getProperty("KAFKA_ENABLE_IDEMPOTENCE"));
        properties.put(ProducerConfig.ACKS_CONFIG, env.getProperty("KAFKA_PRODUCER_ACKS"));
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("KAFKA_SCHEMA_REGISTRY"));
        properties.put(AUTO_REGISTER_SCHEMAS_CONFIG, env.getProperty("KAFKA_SCHEMA_AUTO_REGISTER"));
        return properties;
    }
}
