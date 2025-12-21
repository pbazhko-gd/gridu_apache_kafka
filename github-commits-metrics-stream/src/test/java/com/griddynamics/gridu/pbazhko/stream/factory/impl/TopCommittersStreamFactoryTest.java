package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.model.TopCommitersModel;
import com.griddynamics.gridu.pbazhko.model.TopCommitersModel.CommitterModel;
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

@ContextConfiguration(classes = TopCommittersStreamFactory.class)
class TopCommittersStreamFactoryTest extends BaseGitHubCommitsMetricsTopology<TopCommitersModel> {

    @Autowired
    private TopCommittersStreamFactory streamFactory;

    @Autowired
    private KafkaJsonSchemaSerde<TopCommitersModel> topCommittersKafkaJsonSchemaSerde;

    @Value("${TOP_COMMITTERS_TOPIC}")
    private String topCommittersTopic;

    @Value("${TOP_COMMITTERS_GROUP_SIZE}")
    private int topCommittersGroupSize;

    @Value("${TOP_COMMITTERS_KEY}")
    private String topCommittersKey;

    @Override
    protected GitHubCommitsMetricsStreamFactory getStreamFactory() {
        return streamFactory;
    }

    @Override
    protected TestOutputTopic<String, TopCommitersModel> getOutputTopic() {
        return testDriver.createOutputTopic(
            topCommittersTopic,
            stringSerde.deserializer(),
            topCommittersKafkaJsonSchemaSerde.deserializer()
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

        KeyValue<String, TopCommitersModel> kv;

        kv = outputTopic.readKeyValue();
        assertEquals(topCommittersKey, kv.key);
        assertEquals(TopCommitersModel.of(
            new CommitterModel("user1", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(topCommittersKey, kv.key);
        assertEquals(TopCommitersModel.of(
            new CommitterModel("user1", 1L),
            new CommitterModel("user2", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(topCommittersKey, kv.key);
        assertEquals(TopCommitersModel.of(
            new CommitterModel("user1", 2L),
            new CommitterModel("user2", 1L)
        ), kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(topCommittersKey, kv.key);
        assertEquals(TopCommitersModel.of(
            new CommitterModel("user1", 2L),
            new CommitterModel("user2", 2L)
        ), kv.value);

        assertTrue(outputTopic.isEmpty());
    }
}
