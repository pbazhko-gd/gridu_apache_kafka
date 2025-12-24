package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.stream.factory.AbstractGitHubCommitsMetricsStreamFactory;
import com.griddynamics.gridu.pbazhko.stream.processor.TotalCommittersCountProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TotalCommittersCountStreamFactory extends AbstractGitHubCommitsMetricsStreamFactory {

    @Value("${TOTAL_COMMITTERS_COUNT_STREAMS_APPLICATION_ID}")
    private String totalCommittersCountStreamsApplicationId;

    @Value("${TOTAL_COMMITTERS_COUNT_TOPIC}")
    private String totalCommittersCountTopic;

    @Value("${TOTAL_COMMITTERS_COUNT_STATE_STORE}")
    private String totalCommittersCountStateStore;

    @Value("${TOTAL_COMMITTERS_COUNT_KEY}")
    private String totalCommittersCountKey;

    @Override
    protected String getApplicationId() {
        return totalCommittersCountStreamsApplicationId;
    }

    @Override
    public Topology getStreamTopology() {
        var streamsBuilder = new StreamsBuilder();

        var commitsKStream = getGitHubCommitsKStream(streamsBuilder, true);

        streamsBuilder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(totalCommittersCountStateStore),
                stringSerde,
                longSerde
            )
        );

        var uniqueUsersCountKStream = commitsKStream.process(
            () -> new TotalCommittersCountProcessor(totalCommittersCountStateStore, totalCommittersCountKey),
            totalCommittersCountStateStore
        );

        uniqueUsersCountKStream.to(
            totalCommittersCountTopic,
            Produced.with(stringSerde, longSerde)
        );

        return streamsBuilder.build();
    }
}
