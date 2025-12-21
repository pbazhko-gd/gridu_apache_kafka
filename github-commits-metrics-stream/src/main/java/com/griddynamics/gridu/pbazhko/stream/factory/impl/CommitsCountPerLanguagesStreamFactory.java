package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.model.LanguagesMetricModel;
import com.griddynamics.gridu.pbazhko.stream.factory.AbstractGitHubCommitsMetricsStreamFactory;
import com.griddynamics.gridu.pbazhko.stream.processor.CommitsPerLanguageProcessor;
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
public class CommitsCountPerLanguagesStreamFactory extends AbstractGitHubCommitsMetricsStreamFactory {

    @Value("${COMMITS_COUNT_PER_LANGUAGE_TOPIC}")
    private String commitsCountPerLanguageTopic;

    @Value("${TOP_LANGUAGES_TOPIC}")
    private String topLanguagesTopic;

    @Value("${TOP_LANGUAGES_GROUP_SIZE}")
    private int topLanguagesGroupSize;

    @Value("${COMMITS_COUNT_PER_LANGUAGE_STATE_STORE}")
    private String commitsCountPerLanguageStateStoreName;

    @Value("${TOP_LANGUAGES_CURRENT_TOP_STATE_STORE}")
    private String topLanguagesCurrentTopStateStoreName;

    @Value("${COMMITS_COUNT_PER_LANGUAGE_KEY}")
    private String commitsCountPerLanguageKey;

    @Value("${TOP_LANGUAGES_KEY}")
    private String topLanguagesKey;

    @Value("${COMMITS_COUNT_PER_LANGUAGE_STREAMS_APPLICATION_ID}")
    private String commitsCountPerLanguageStreamsApplicationId;

    @Autowired
    private KafkaJsonSchemaSerde<LanguagesMetricModel> languagesMetricModelKafkaJsonSchemaSerde;

    @Override
    public String getApplicationId() {
        return commitsCountPerLanguageStreamsApplicationId;
    }

    @Override
    public Topology getStreamTopology() {
        var streamsBuilder = new StreamsBuilder();

        var commitsKStream = getGitHubCommitsKStream(streamsBuilder, true);

        var commitsPerLanguageKTable = commitsKStream
            .selectKey((key, value) -> value.getLanguage())
            .map((key, value) -> KeyValue.pair(key, 1L))
            .groupByKey(Grouped.with(stringSerde, longSerde))
            .count(Materialized.as(commitsCountPerLanguageStateStoreName));

        // Recalculate commits per language metrics
        var commitsPerLanguageKStream = commitsPerLanguageKTable.toStream()
            .process(
                () -> new CommitsPerLanguageProcessor(commitsCountPerLanguageStateStoreName, commitsCountPerLanguageKey),
                commitsCountPerLanguageStateStoreName
            );

        commitsPerLanguageKStream.to(
            commitsCountPerLanguageTopic,
            Produced.with(stringSerde, languagesMetricModelKafkaJsonSchemaSerde)
        );

        // Recalculate TOP languages metrics
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
            Produced.with(stringSerde, languagesMetricModelKafkaJsonSchemaSerde)
        );

        return streamsBuilder.build();
    }
}
