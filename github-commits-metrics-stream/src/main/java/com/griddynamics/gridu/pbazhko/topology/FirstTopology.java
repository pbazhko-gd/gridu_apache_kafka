package com.griddynamics.gridu.pbazhko.topology;

import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class FirstTopology implements GitHubCommitsMetricsTopology {

    @Value("${KAFKA_GITHUB_COMMITS_TOPIC}")
    private String gitHubCommitsTopic;

    @Value("${KAFKA_GITHUB_METRICS_TOPIC}")
    private String gitHubMetricsTopic;

    private final KafkaJsonSchemaSerde<GitHubCommit> gitHubCommitKafkaJsonSchemaSerde;

    @Override
    public Topology getTopology() {
        var streamsBuilder = new StreamsBuilder();
        var input = streamsBuilder.stream(
            gitHubCommitsTopic,
            Consumed.with(Serdes.String(), gitHubCommitKafkaJsonSchemaSerde)
        );
        var upper = input.mapValues(GitHubCommit::toString);
        upper.to(
            gitHubMetricsTopic,
            Produced.with(Serdes.String(), Serdes.String())
        );
        return streamsBuilder.build();
    }
}
