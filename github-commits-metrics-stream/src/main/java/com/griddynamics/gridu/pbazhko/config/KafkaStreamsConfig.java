package com.griddynamics.gridu.pbazhko.config;

import com.griddynamics.gridu.pbazhko.topology.GitHubCommitsMetricsTopology;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.List;
import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class KafkaStreamsConfig {

    private final Environment env;

    @Bean
    public List<KafkaStreams> kafkaStreams(List<GitHubCommitsMetricsTopology> topologies) {
        var streamsProperties = getStreamsProperties();
        return topologies.stream()
            .map(t -> {
                var kafkaStreams = new KafkaStreams(t.getTopology(), streamsProperties);
                kafkaStreams.start();
                return kafkaStreams;
            }).toList();
    }

    private Properties getStreamsProperties() {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, env.getProperty("KAFKA_STREAMS_APPLICATION_ID"));
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaJsonSchemaSerde.class);
        return properties;
    }
}
