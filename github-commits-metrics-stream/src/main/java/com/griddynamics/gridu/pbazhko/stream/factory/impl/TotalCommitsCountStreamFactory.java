package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.stream.factory.AbstractGitHubCommitsMetricsStreamFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TotalCommitsCountStreamFactory extends AbstractGitHubCommitsMetricsStreamFactory {

    @Value("${TOTAL_COMMITS_COUNT_TOPIC}")
    private String totalCommitsCountTopic;

    @Value("${TOTAL_COMMITS_COUNT_STATE_STORE}")
    private String totalCommitsCountStateStore;

    @Value("${TOTAL_COMMITS_COUNT_KEY}")
    private String totalCommitsCountKey;

    @Value("${TOTAL_COMMITS_COUNT_STREAMS_APPLICATION_ID}")
    private String totalCommitsCountStreamsApplicationId;

    @Override
    public String getApplicationId() {
        return totalCommitsCountStreamsApplicationId;
    }

    @Override
    public Topology getStreamTopology() {
        var streamsBuilder = new StreamsBuilder();
        var commitsKStream = getGitHubCommitsKStream(streamsBuilder, true);

        var totalCommitsKTable = commitsKStream
            .map((key, value) -> KeyValue.pair(totalCommitsCountKey, 1L))
            .groupByKey(Grouped.with(stringSerde, longSerde))
            .count(Materialized.as(totalCommitsCountStateStore));

        totalCommitsKTable.toStream().to(
            totalCommitsCountTopic,
            Produced.with(stringSerde, longSerde)
        );

        return streamsBuilder.build();
    }
}
