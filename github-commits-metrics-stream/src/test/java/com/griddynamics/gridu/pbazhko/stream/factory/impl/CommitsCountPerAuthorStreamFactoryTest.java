package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.model.metrics.CommitersMetricModel;
import com.griddynamics.gridu.pbazhko.model.metrics.CommitersMetricModel.CommitterMetricRecord;
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

@ContextConfiguration(classes = CommitsCountPerAuthorStreamFactory.class)
class CommitsCountPerAuthorStreamFactoryTest extends BaseGitHubCommitsMetricsTopology<CommitersMetricModel> {

    @Autowired
    private CommitsCountPerAuthorStreamFactory streamFactory;

    @Autowired
    private KafkaJsonSchemaSerde<CommitersMetricModel> committersMetricModelKafkaJsonSchemaSerde;

    @Value("${COMMITS_COUNT_PER_AUTHOR_TOPIC}")
    private String commitsCountPerAuthorTopic;

    @Value("${COMMITS_COUNT_PER_AUTHOR_KEY}")
    private String commitsCountPerAuthorKey;

    @Override
    protected GitHubCommitsMetricsStreamFactory getStreamFactory() {
        return streamFactory;
    }

    @Override
    protected TestOutputTopic<String, CommitersMetricModel> getOutputTopic() {
        return testDriver.createOutputTopic(
            commitsCountPerAuthorTopic,
            stringSerde.deserializer(),
            committersMetricModelKafkaJsonSchemaSerde.deserializer()
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

        KeyValue<String, CommitersMetricModel> kv;

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerAuthorKey, kv.key);
        assertEquals(CommitersMetricModel.of(
            new CommitterMetricRecord("user1", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerAuthorKey, kv.key);
        assertEquals(CommitersMetricModel.of(
            new CommitterMetricRecord("user1", 1L),
            new CommitterMetricRecord("user2", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerAuthorKey, kv.key);
        assertEquals(CommitersMetricModel.of(
            new CommitterMetricRecord("user1", 2L),
            new CommitterMetricRecord("user2", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerAuthorKey, kv.key);
        assertEquals(CommitersMetricModel.of(
            new CommitterMetricRecord("user1", 2L),
            new CommitterMetricRecord("user2", 1L),
            new CommitterMetricRecord("user3", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(commitsCountPerAuthorKey, kv.key);
        assertEquals(CommitersMetricModel.of(
            new CommitterMetricRecord("user1", 2L),
            new CommitterMetricRecord("user2", 2L),
            new CommitterMetricRecord("user3", 1L)
        ), kv.value);

        assertTrue(outputTopic.isEmpty());
    }
}
