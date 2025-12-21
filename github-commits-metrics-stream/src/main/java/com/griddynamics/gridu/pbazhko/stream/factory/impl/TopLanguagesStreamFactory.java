package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.model.LanguagesModel;
import com.griddynamics.gridu.pbazhko.stream.factory.AbstractGitHubCommitsMetricsStreamFactory;
import com.griddynamics.gridu.pbazhko.stream.processor.TopLanguagesProcessor;
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
public class TopLanguagesStreamFactory extends AbstractGitHubCommitsMetricsStreamFactory {

    @Value("${TOP_LANGUAGES_TOPIC}")
    private String topLanguagesTopic;

    @Value("${TOP_LANGUAGES_GROUP_SIZE}")
    private int topLanguagesGroupSize;

    @Value("${TOP_LANGUAGES_CURRENT_TOP_STATE_STORE}")
    private String topLanguagesCurrentTopStateStoreName;

    @Value("${TOP_LANGUAGES_COMMITS_PER_LANGUAGE_STATE_STORE}")
    private String commitsPerLanguageStateStoreName;

    @Value("${TOP_LANGUAGES_KEY}")
    private String topLanguagesKey;

    @Value("${TOP_LANGUAGES_STREAMS_APPLICATION_ID}")
    private String topLanguagesStreamApplicationId;

    @Autowired
    private KafkaJsonSchemaSerde<LanguagesModel> topLanguagesKafkaJsonSchemaSerde;

    @Override
    public String getApplicationId() {
        return topLanguagesStreamApplicationId;
    }

    @Override
    public Topology getStreamTopology() {
        var streamsBuilder = new StreamsBuilder();

        var commitsKStream = getGitHubCommitsKStream(streamsBuilder, true);

        var commitsPerLanguageKTable = commitsKStream
            .selectKey((key, value) -> value.getLanguage())
            .map((key, value) -> KeyValue.pair(key, 1L))
            .groupByKey(Grouped.with(stringSerde, longSerde))
            .count(Materialized.as(commitsPerLanguageStateStoreName));

        streamsBuilder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(topLanguagesCurrentTopStateStoreName),
                stringSerde,
                longSerde
            )
        );

        var topLanguagesKStream = commitsPerLanguageKTable.toStream()
            .process(
                () -> new TopLanguagesProcessor(
                    topLanguagesCurrentTopStateStoreName,
                    topLanguagesGroupSize,
                    topLanguagesKey
                ),
                topLanguagesCurrentTopStateStoreName
            );

        topLanguagesKStream.to(
            topLanguagesTopic,
            Produced.with(stringSerde, topLanguagesKafkaJsonSchemaSerde)
        );

        return streamsBuilder.build();
    }
}
