package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.config.AppConfig;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.testcontainers.BaseKafkaTestContainerTest;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = AppConfig.class,
    initializers = GitHubAccountsPublishingServiceTest.TestEnvInitializer.class
)
class GitHubAccountsPublishingServiceTest extends BaseKafkaTestContainerTest {

    public static final String TOPIC = "test-github-accounts";
    public static final String SCHEMA = "github-account-schema.json";

    @Autowired
    private GitHubAccountsPublishingService gitHubAccountsPublishingService;

    private static KafkaConsumer<String, GitHubAccount> kafkaConsumer;

    @BeforeAll
    static void setup() throws IOException, InterruptedException, URISyntaxException {
        createTopic(TOPIC);

        try (var is = GitHubAccountsPublishingServiceTest.class
            .getClassLoader()
            .getResourceAsStream(SCHEMA)
        ) {
            if (is == null) {
                throw new IllegalStateException("Resource " + SCHEMA + " not found");
            }

            var schema = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            createSchema(schema, TOPIC);
        }

        kafkaConsumer = getConsumer(TOPIC, GitHubAccount.class);
    }

    @AfterAll
    static void tearDown() {
        deleteTopic(TOPIC);
        kafkaConsumer.close();
    }

    @Test
    void test_publish() {
        // when
        gitHubAccountsPublishingService.publish();
        // then
        var expectedAccount1 = new GitHubAccount("user1", "1d");
        var expectedAccount2 = new GitHubAccount("user2", "2w");
        var records = StreamSupport.stream(
            kafkaConsumer
                .poll(Duration.ofSeconds(2))
                .spliterator(),
            false
        ).toList();
        assertEquals(2, records.size());
        assertEquals(expectedAccount1, records.get(0).value());
        assertEquals(expectedAccount2, records.get(1).value());
    }

    static class TestEnvInitializer implements ApplicationContextInitializer<@NonNull ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            var testProps = new HashMap<String, Object>();
            testProps.put("KAFKA_BOOTSTRAP_SERVERS", getBootstrapServers());
            testProps.put("KAFKA_SCHEMA_REGISTRY", getSchemaRegistry());
            testProps.put("KAFKA_GITHUB_ACCOUNTS_TOPIC", TOPIC);
            testProps.put("GITHUB_ACCOUNTS_LIST_FILENAME", "test-github-accounts.txt");

            applicationContext.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("testProps", testProps));
        }
    }
}
