package com.griddynamics.gridu.pbazhko.topology;

import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import com.griddynamics.gridu.pbazhko.transformer.TopFiveCommittersTransformer;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopContributorsTopology implements GitHubCommitsMetricsTopology {

    @Value("${KAFKA_GITHUB_COMMITS_TOPIC}")
    private String gitHubCommitsTopic;

    @Value("${KAFKA_GITHUB_METRICS_TOPIC}")
    private String gitHubMetricsTopic;

    private final Environment env;
    private final KafkaJsonSchemaSerde<GitHubCommit> gitHubCommitKafkaJsonSchemaSerde;

    private static final String COMMITS_BY_AUTHOR_STORE = "CommitsByAuthor";

    @Override
    public Topology getTopology() {
        var streamsBuilder = new StreamsBuilder();
        var totalCommitsNumber = streamsBuilder.stream(
                gitHubCommitsTopic,
                Consumed.with(Serdes.String(), gitHubCommitKafkaJsonSchemaSerde)
            ).selectKey((k, v) -> v.getAuthor())
            .groupByKey()
            .count(Materialized.as(COMMITS_BY_AUTHOR_STORE))
            .toStream()
            .transform(() -> new TopFiveCommittersTransformer(COMMITS_BY_AUTHOR_STORE), COMMITS_BY_AUTHOR_STORE);
        totalCommitsNumber.to(gitHubMetricsTopic, Produced.with(Serdes.String(), Serdes.String()));
        return streamsBuilder.build();
    }

    @Override
    public Properties getCustomProperties() {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, env.getProperty("TOP_COMMITTERS_STREAMS_APPLICATION_ID"));
        return properties;
    }
}
