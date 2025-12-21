package com.griddynamics.gridu.pbazhko.stream.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import com.griddynamics.gridu.pbazhko.stream.processor.CommitsDeduplicationProcessor;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;

import java.util.Properties;

public abstract class AbstractGitHubCommitsMetricsStreamFactory implements GitHubCommitsMetricsStreamFactory {

    @Value("${KAFKA_GITHUB_COMMITS_TOPIC}")
    protected String gitHubCommitsTopic;

    @Value("${UNIQUE_SHA_STATE_STORE}")
    protected String uniqueShaStateStore;

    @Autowired
    protected Environment env;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected KafkaJsonSchemaSerde<GitHubCommit> gitHubCommitKafkaJsonSchemaSerde;

    protected final Serde<String> stringSerde = Serdes.String();
    protected final Serde<Long> longSerde = Serdes.Long();
    protected final Serde<Boolean> booleanSerde = Serdes.Boolean();

    protected abstract String getApplicationId();

    @Override
    public Properties getStreamProperties() {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
        return properties;
    }

    protected final KStream<String, GitHubCommit> getGitHubCommitsKStream(
        StreamsBuilder streamsBuilder,
        boolean withShaDeduplication
    ) {
        var gitHubCommitsKStream = streamsBuilder.stream(
            gitHubCommitsTopic,
            Consumed.with(stringSerde, gitHubCommitKafkaJsonSchemaSerde)
        );
        if (withShaDeduplication) {
            createDeduplicationStateStore(streamsBuilder);
            return gitHubCommitsKStream
                .process(() -> new CommitsDeduplicationProcessor(uniqueShaStateStore), uniqueShaStateStore);
        } else {
            return gitHubCommitsKStream;
        }
    }

    private void createDeduplicationStateStore(StreamsBuilder streamsBuilder) {
        streamsBuilder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(uniqueShaStateStore),
                stringSerde,
                booleanSerde
            )
        );
    }
}
