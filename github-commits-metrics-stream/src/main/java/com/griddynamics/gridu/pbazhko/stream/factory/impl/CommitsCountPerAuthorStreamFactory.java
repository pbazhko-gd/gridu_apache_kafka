package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.model.metrics.CommitersMetricModel;
import com.griddynamics.gridu.pbazhko.stream.processor.CommitsPerAuthorProcessor;
import com.griddynamics.gridu.pbazhko.stream.processor.TopCommittersProcessor;
import com.griddynamics.gridu.pbazhko.stream.factory.AbstractGitHubCommitsMetricsStreamFactory;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class CommitsCountPerAuthorStreamFactory extends AbstractGitHubCommitsMetricsStreamFactory {

    @Value("${COMMITS_COUNT_PER_AUTHOR_TOPIC}")
    private String commitsCountPerAuthorTopic;

    @Value("${TOP_COMMITTERS_TOPIC}")
    private String topCommittersTopic;

    @Value("${TOP_COMMITTERS_GROUP_SIZE}")
    private int topCommittersGroupSize;

    @Value("${TOP_COMMITTERS_CURRENT_TOP_STATE_STORE}")
    private String topCommittersCurrentTopStateStoreName;

    @Value("${COMMITS_COUNT_PER_AUTHOR_STATE_STORE}")
    private String commitsCountPerAuthorStateStoreName;

    @Value("${COMMITS_COUNT_PER_AUTHOR_KEY}")
    private String commitsCountPerAuthorKey;

    @Value("${TOP_COMMITTERS_KEY}")
    private String topCommittersKey;

    @Value("${COMMITS_COUNT_PER_AUTHOR_STREAMS_APPLICATION_ID}")
    private String commitsCountPerAuthorStreamsApplicationId;

    @Autowired
    private KafkaJsonSchemaSerde<CommitersMetricModel> committersMetricModelKafkaJsonSchemaSerde;

    @Override
    public String getApplicationId() {
        return commitsCountPerAuthorStreamsApplicationId;
    }

    @Override
    public Topology getStreamTopology() {
        var streamsBuilder = new StreamsBuilder();

        var commitsKStream = getGitHubCommitsKStream(streamsBuilder, true);

        var commitsPerUserKTable = commitsKStream
            .selectKey((key, value) -> value.getAuthor())
            .map((key, value) -> KeyValue.pair(key, 1L))
            .groupByKey(Grouped.with(stringSerde, longSerde))
            .count(Materialized.as(commitsCountPerAuthorStateStoreName));

        // Recalculate commits per author metrics
        var commitsPerAuthorKStream = commitsPerUserKTable.toStream()
            .process(
                () -> new CommitsPerAuthorProcessor(commitsCountPerAuthorStateStoreName, commitsCountPerAuthorKey),
                commitsCountPerAuthorStateStoreName
            );

        commitsPerAuthorKStream.to(
            commitsCountPerAuthorTopic,
            Produced.with(stringSerde, committersMetricModelKafkaJsonSchemaSerde)
        );

        // Recalculate TOP committers metrics
        streamsBuilder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(topCommittersCurrentTopStateStoreName),
                stringSerde,
                longSerde
            )
        );

        var topCommittersKStream = commitsPerUserKTable.toStream()
            .process(
                () -> new TopCommittersProcessor(
                    topCommittersCurrentTopStateStoreName,
                    topCommittersGroupSize,
                    topCommittersKey
                ),
                topCommittersCurrentTopStateStoreName
            );

        topCommittersKStream.to(
            topCommittersTopic,
            Produced.with(stringSerde, committersMetricModelKafkaJsonSchemaSerde)
        );

        return streamsBuilder.build();
    }
}
