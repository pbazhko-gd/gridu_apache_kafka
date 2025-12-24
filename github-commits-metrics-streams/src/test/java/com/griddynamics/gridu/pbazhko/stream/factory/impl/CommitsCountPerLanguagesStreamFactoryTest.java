package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.model.metrics.LanguagesMetricModel;
import com.griddynamics.gridu.pbazhko.model.metrics.LanguagesMetricModel.LanguageMetricRecord;
import com.griddynamics.gridu.pbazhko.stream.BaseGitHubCommitsMetricsTopology;
import com.griddynamics.gridu.pbazhko.stream.factory.GitHubCommitsMetricsStreamFactory;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ContextConfiguration(classes = CommitsCountPerLanguagesStreamFactory.class)
class CommitsCountPerLanguagesStreamFactoryTest extends BaseGitHubCommitsMetricsTopology<LanguagesMetricModel> {

    @Autowired
    private CommitsCountPerLanguagesStreamFactory streamFactory;

    @Autowired
    private KafkaJsonSchemaSerde<LanguagesMetricModel> languagesMetricModelKafkaJsonSchemaSerde;

    @Value("${COMMITS_COUNT_PER_LANGUAGE_TOPIC}")
    private String commitsCountPerLanguageTopic;

    @Value("${COMMITS_COUNT_PER_LANGUAGE_KEY}")
    private String commitsCountPerLanguageKey;

    @Override
    protected GitHubCommitsMetricsStreamFactory getStreamFactory() {
        return streamFactory;
    }

    @Override
    protected TestOutputTopic<String, LanguagesMetricModel> getOutputTopic() {
        return testDriver.createOutputTopic(
            commitsCountPerLanguageTopic,
            stringSerde.deserializer(),
            languagesMetricModelKafkaJsonSchemaSerde.deserializer()
        );
    }

    @Test
    void testOutputWhenCommitsPublished() {
        publishCommits(List.of(
            ShortCommit.of("user1", "java", "sha1"),
            ShortCommit.of("user2", "kotlin", "sha2"),
            ShortCommit.of("user1", "python", "sha3"),
            ShortCommit.of("user3", "kotlin", "sha4"),
            ShortCommit.of("user2", "scala", "sha5"),
            ShortCommit.of("user3", "kotlin", "sha4") // duplicate sha
        ));

        KeyValue<String, LanguagesMetricModel> kv;

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerLanguageKey, kv.key);
        assertEquals(LanguagesMetricModel.of(
            new LanguageMetricRecord("java", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerLanguageKey, kv.key);
        assertEquals(LanguagesMetricModel.of(
            new LanguageMetricRecord("java", 1L),
            new LanguageMetricRecord("kotlin", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerLanguageKey, kv.key);
        assertEquals(LanguagesMetricModel.of(
            new LanguageMetricRecord("java", 1L),
            new LanguageMetricRecord("kotlin", 1L),
            new LanguageMetricRecord("python", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerLanguageKey, kv.key);
        assertEquals(LanguagesMetricModel.of(
            new LanguageMetricRecord("java", 1L),
            new LanguageMetricRecord("kotlin", 2L),
            new LanguageMetricRecord("python", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerLanguageKey, kv.key);
        assertEquals(LanguagesMetricModel.of(
            new LanguageMetricRecord("java", 1L),
            new LanguageMetricRecord("kotlin", 2L),
            new LanguageMetricRecord("python", 1L),
            new LanguageMetricRecord("scala", 1L)
        ), kv.value);

        assertTrue(outputTopic.isEmpty());
    }
}
