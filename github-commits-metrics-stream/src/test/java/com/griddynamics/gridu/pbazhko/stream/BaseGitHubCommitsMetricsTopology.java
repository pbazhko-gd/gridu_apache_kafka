package com.griddynamics.gridu.pbazhko.stream;

import com.griddynamics.gridu.pbazhko.config.AppConfig;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import com.griddynamics.gridu.pbazhko.stream.factory.GitHubCommitsMetricsStreamFactory;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.With;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

import static com.griddynamics.gridu.pbazhko.util.CommonUtils.mergeProperties;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = {
        AppConfig.class,
        TestConfig.class
    },
    initializers = TestEnvInitializer.class
)
// P - generic value type in output topic
public abstract class BaseGitHubCommitsMetricsTopology<P> {

    protected TopologyTestDriver testDriver;
    protected TestInputTopic<String, GitHubCommit> inputTopic;
    protected TestOutputTopic<String, P> outputTopic;

    @Value("${KAFKA_GITHUB_COMMITS_TOPIC}")
    private String gitHubCommitsTopic;

    @Autowired
    @Qualifier("baseKafkaStreamProperties")
    private Properties baseKafkaStreamProperties;

    @Autowired
    private KafkaJsonSchemaSerde<GitHubCommit> gitHubCommitKafkaJsonSchemaSerde;

    protected final Serde<String> stringSerde = Serdes.String();
    protected final Serde<Long> longSerde = Serdes.Long();

    protected abstract GitHubCommitsMetricsStreamFactory getStreamFactory();

    protected abstract TestOutputTopic<String, P> getOutputTopic();

    @BeforeEach
    protected void setup() {
        var topology = getStreamFactory().getStreamTopology();

        testDriver = new TopologyTestDriver(
            topology,
            mergeProperties(baseKafkaStreamProperties, getStreamFactory().getStreamProperties())
        );

        inputTopic = testDriver.createInputTopic(
            gitHubCommitsTopic,
            stringSerde.serializer(),
            gitHubCommitKafkaJsonSchemaSerde.serializer()
        );

        outputTopic = getOutputTopic();
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testEmptyOutputWhenNoCommits() {
        assertTrue(outputTopic.isEmpty());
    }

    protected final void publishCommits(List<ShortCommit> data) {
        data.forEach(commit -> {
            inputTopic.pipeInput(commit.sha, buildGitHubCommit(commit));
        });
    }

    private GitHubCommit buildGitHubCommit(ShortCommit commit) {
        return GitHubCommit.builder()
            .author(commit.author)
            .language(commit.language)
            .dateTimeUtc(LocalDateTime.now())
            .sha(commit.sha)
            .repositoryFullName(RandomStringUtils.randomAlphanumeric(16))
            .message(RandomStringUtils.randomAlphanumeric(24))
            .build();
    }

    @Getter
    @AllArgsConstructor
    protected static class ShortCommit {
        private String author;
        private String language;
        private String sha;

        public static ShortCommit of(String author, String language) {
            return new ShortCommit(author, language, RandomStringUtils.randomAlphanumeric(8));
        }

        public static ShortCommit of(String author, String language, String sha) {
            return new ShortCommit(author, language, sha);
        }
    }
}
