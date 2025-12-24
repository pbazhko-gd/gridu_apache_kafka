package com.griddynamics.gridu.pbazhko.stream.factory.impl;

import com.griddynamics.gridu.pbazhko.stream.BaseGitHubCommitsMetricsTopology;
import com.griddynamics.gridu.pbazhko.stream.factory.GitHubCommitsMetricsStreamFactory;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ContextConfiguration(classes = TotalCommitsCountStreamFactory.class)
class TotalCommitsCountTopologyTest extends BaseGitHubCommitsMetricsTopology<Long> {

    @Autowired
    private TotalCommitsCountStreamFactory streamFactory;

    @Value("${TOTAL_COMMITS_COUNT_TOPIC}")
    private String totalCommitsCountTopic;

    @Value("${TOTAL_COMMITS_COUNT_KEY}")
    private String totalCommitsCountKey;

    @Override
    protected GitHubCommitsMetricsStreamFactory getStreamFactory() {
        return streamFactory;
    }

    @Override
    protected TestOutputTopic<String, Long> getOutputTopic() {
        return testDriver.createOutputTopic(
            totalCommitsCountTopic,
            stringSerde.deserializer(),
            longSerde.deserializer()
        );
    }

    @Test
    void testOutputWhenCommitsPublished() {
        publishCommits(List.of(
            ShortCommit.of("user1", "java", "sha1"),
            ShortCommit.of("user2", "kotlin", "sha2"),
            ShortCommit.of("user1", "python", "sha3"),
            ShortCommit.of("user1", "java", "sha1") // duplicate sha
        ));

        KeyValue<String, Long> kv;

        kv = outputTopic.readKeyValue();
        assertEquals(totalCommitsCountKey, kv.key);
        assertEquals(1, kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(totalCommitsCountKey, kv.key);
        assertEquals(2, kv.value);

        kv = outputTopic.readKeyValue();
        assertEquals(totalCommitsCountKey, kv.key);
        assertEquals(3, kv.value);

        assertTrue(outputTopic.isEmpty());
    }
}
