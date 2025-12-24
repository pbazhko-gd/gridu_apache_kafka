package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.stream.BaseGitHubCommitsMetricsTopology;
import com.griddynamics.gridu.pbazhko.stream.factory.GitHubCommitsMetricsStreamFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ContextConfiguration(classes = TotalCommittersCountStreamFactory.class)
class TotalCommittersCountStreamFactoryTest extends BaseGitHubCommitsMetricsTopology<Long> {

    @Autowired
    private TotalCommittersCountStreamFactory streamFactory;

    @Value("${TOTAL_COMMITTERS_COUNT_TOPIC}")
    private String totalCommittersCountTopic;

    @Value("${TOTAL_COMMITTERS_COUNT_KEY}")
    private String totalCommittersCountKey;

    @Override
    protected GitHubCommitsMetricsStreamFactory getStreamFactory() {
        return streamFactory;
    }

    @Override
    protected TestOutputTopic<String, Long> getOutputTopic() {
        return testDriver.createOutputTopic(
            totalCommittersCountTopic,
            stringSerde.deserializer(),
            longSerde.deserializer()
        );
    }

    @Test
    void testOutputWhenCommitsPublished() {
        publishCommits(List.of(
            ShortCommit.of("user1", "java"),
            ShortCommit.of("user2", "kotlin"),
            ShortCommit.of("user1", "python"),
            ShortCommit.of("user3", "kotlin"),
            ShortCommit.of("user2", "scala")
        ));

        KeyValue<String, Long> kv;

        kv = outputTopic.readKeyValue();
        assertEquals(totalCommittersCountKey, kv.key);
        assertEquals(1, kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(totalCommittersCountKey, kv.key);
        assertEquals(2, kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(totalCommittersCountKey, kv.key);
        assertEquals(3, kv.value);

        assertTrue(outputTopic.isEmpty());
    }
}
