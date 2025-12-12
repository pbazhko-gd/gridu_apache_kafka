package com.griddynamics.gridu.pbazhko.config;

import com.griddynamics.gridu.pbazhko.topology.GitHubCommitsMetricsTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.List;
import java.util.Properties;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaStreamsConfig {

    @Bean
    public List<KafkaStreams> kafkaStreams(List<GitHubCommitsMetricsTopology> topologies, Environment env) {
        var baseStreamsProperties = getBaseKafkaStreamsProperties(env);
        var streamsUncaughtExceptionHandler = new StreamsUncaughtExceptionHandler() {
            @Override
            public StreamThreadExceptionResponse handle(Throwable throwable) {
                log.error("Stream thread failure", throwable);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        };
        return topologies.stream()
            .map(t -> {
                var kafkaStreams = new KafkaStreams(
                    t.getTopology(),
                    mergeProperties(baseStreamsProperties, t.getCustomProperties())
                );
                kafkaStreams.setUncaughtExceptionHandler(streamsUncaughtExceptionHandler);
                kafkaStreams.start();
                return kafkaStreams;
            }).toList();
    }

    private Properties getBaseKafkaStreamsProperties(Environment env) {
        var properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // Skip records with output topic write errors
        properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, DefaultProductionExceptionHandler.class);

        // Skip records with deserialization errors
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return properties;
    }

    private static Properties mergeProperties(Properties p1, Properties p2) {
        var p = new Properties();
        p.putAll(p1);
        p.putAll(p2);
        return p;
    }
}
