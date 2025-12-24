package com.griddynamics.gridu.pbazhko.config;

import com.griddynamics.gridu.pbazhko.stream.factory.GitHubCommitsMetricsStreamFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Properties;

import static com.griddynamics.gridu.pbazhko.util.CommonUtils.mergeProperties;

@Slf4j
@Configuration
@RequiredArgsConstructor
@ComponentScan(basePackages = "com.griddynamics.gridu.pbazhko")
public class KafkaStreamsConfig {

    @Bean
    public List<KafkaStreams> kafkaStreams(
        List<GitHubCommitsMetricsStreamFactory> topologies,
        @Qualifier("baseKafkaStreamProperties") Properties baseKafkaStreamProperties
    ) {
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
                    t.getStreamTopology(),
                    mergeProperties(baseKafkaStreamProperties, t.getStreamProperties())
                );
                kafkaStreams.setUncaughtExceptionHandler(streamsUncaughtExceptionHandler);
                kafkaStreams.start();
                return kafkaStreams;
            }).toList();
    }
}
