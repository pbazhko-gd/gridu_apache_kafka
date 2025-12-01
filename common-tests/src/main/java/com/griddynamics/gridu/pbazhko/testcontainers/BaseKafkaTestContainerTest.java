package com.griddynamics.gridu.pbazhko.testcontainers;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Slf4j
public abstract class BaseKafkaTestContainerTest {

    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.9.4";
    private static final String KAFKA_SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.9.4";

    private static final int KAFKA_SCHEMA_REGISTRY_PORT = 8081;
    private static final int KAFKA_PORT = 49092;

    private static final Network NETWORK = Network.newNetwork();

    private static final ConfluentKafkaContainer KAFKA_CONTAINER =
        new ConfluentKafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
            .withNetwork(NETWORK)
            .withListener("kafka:" + KAFKA_PORT)
            .withNetworkAliases("kafka");

    private static final KafkaSchemaRegistryContainer SCHEMA_REGISTRY_CONTAINER =
        new KafkaSchemaRegistryContainer(DockerImageName.parse(KAFKA_SCHEMA_REGISTRY_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(KAFKA_SCHEMA_REGISTRY_PORT)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:" + KAFKA_PORT)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_CLIENT_LISTENER_NAME", "PLAINTEXT")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "PLAINTEXT")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + KAFKA_SCHEMA_REGISTRY_PORT)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .withStartupTimeout(Duration.of(120, ChronoUnit.SECONDS));

    static {
        KAFKA_CONTAINER.start();
        SCHEMA_REGISTRY_CONTAINER.start();
    }

    protected static String getBootstrapServers() {
        return KAFKA_CONTAINER.getBootstrapServers();
    }

    protected static String getSchemaRegistry() {
        return SCHEMA_REGISTRY_CONTAINER.getSchemaRegistry();
    }

    @SneakyThrows
    protected static void createTopic(String topicName) {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (var adminClient = AdminClient.create(props)) {
            var topic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Collections.singleton(topic)).all().get();
            log.info("Topic {} has been successfully created", topicName);
        }
    }

    @SneakyThrows
    protected static void deleteTopic(String topicName) {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (var adminClient = AdminClient.create(props)) {
            adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
            log.info("Topic {} has been successfully deleted", topicName);
        }
    }

    protected static void createSchema(String schema, String topicName) throws IOException, InterruptedException {
        var client = HttpClient.newHttpClient();
        var objectMapper = new ObjectMapper();
        var params = Map.of(
            "schemaType", "JSON",
            "schema", schema
        );
        var request = HttpRequest.newBuilder()
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .uri(URI.create(getSchemaRegistry() + "/subjects/" + topicName + "-value/versions"))
            .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(params)))
            .build();
        client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    protected static <T> KafkaConsumer<String, T> getConsumer(String topic, Class<T> clazz) {
        var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        consumerProps.put(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistry());
        consumerProps.put("json.value.type", clazz.getName());

        var consumer = new KafkaConsumer<String, T>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
}
