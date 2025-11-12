package com.griddynamics.gridu.pbazhko.factory;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.util.EnvUtil;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaProducerFactory {

    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String AUTO_REGISTER_SCHEMAS_CONFIG = "auto.register.schemas";

    private static final KafkaProducer<String, GitHubAccount> KAFKA_PRODUCER =
        new KafkaProducer<>(getProperties());

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown kafka producer");
            KAFKA_PRODUCER.close();
        }));
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.put(CLIENT_ID_CONFIG, EnvUtil.getConfig("KAFKA_PRODUCER_CLIENT_ID"));
        properties.put(BOOTSTRAP_SERVERS_CONFIG, EnvUtil.getConfig("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, EnvUtil.getConfig("KAFKA_SCHEMA_REGISTRY"));
        properties.put(AUTO_REGISTER_SCHEMAS_CONFIG, EnvUtil.getConfig("KAFKA_SCHEMA_AUTO_REGISTER"));
        return properties;
    }

    public static KafkaProducer<String, GitHubAccount> getProducer() {
        return KAFKA_PRODUCER;
    }
}
