package com.griddynamics.gridu.pbazhko.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.netty.http.client.HttpClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
@Configuration
@PropertySource("classpath:application.properties")
@RequiredArgsConstructor
@ComponentScan(basePackages = "com.griddynamics.gridu.pbazhko")
public class AppConfig {

    private final Environment env;

    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String AUTO_REGISTER_SCHEMAS_CONFIG = "auto.register.schemas";
    private static final String JSON_VALUE_TYPE_CONFIG = "json.value.type";

    @Bean
    public JedisPool jedisPool() {
        return new JedisPool(env.getProperty("REDIS_HOST"), env.getProperty("REDIS_PORT", Integer.class));
    }

    @Bean
    public KafkaReceiver<String, GitHubAccount> kafkaReceiver() {
        var receiverOptions = ReceiverOptions.<String, GitHubAccount>create(getConsumerProperties())
            .subscription(Collections.singletonList(env.getProperty("KAFKA_GITHUB_ACCOUNTS_TOPIC")))
            .addAssignListener(partitions -> {
                log.trace("Partitions assigned: {}", partitions);
            });
        return KafkaReceiver.create(receiverOptions);
    }

    @Bean
    public KafkaProducerHolder kafkaProducerHolder() {
        var kafkaProducer = new KafkaProducer<String, GitHubCommit>(getProducerProperties());
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
        var mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean
    public List<String> blockedGitHubRepositories(
        @Value("classpath:github-repos-blacklist.txt") Resource resource
    ) throws IOException {
        return new ArrayList<>(Files.readAllLines(resource.getFile().toPath()));
    }

    private Properties getConsumerProperties() {
        var properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, env.getProperty("KAFKA_CONSUMER_GROUP_ID"));
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, env.getProperty("KAFKA_CONSUMER_ENABLE_AUTOCOMMIT"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, env.getProperty("KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG"));
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, env.getProperty("KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS"));
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, env.getProperty("KAFKA_CONSUMER_SESSION_TIMEOUT_MS"));
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("KAFKA_SCHEMA_REGISTRY"));
        properties.put(JSON_VALUE_TYPE_CONFIG, GitHubAccount.class.getName());
        return properties;
    }

    private Properties getProducerProperties() {
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
