package com.griddynamics.gridu.pbazhko.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.griddynamics.gridu.pbazhko.config.KafkaProducerHolder;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import com.griddynamics.gridu.pbazhko.model.GitHubSearchResponse;
import com.griddynamics.gridu.pbazhko.util.IntervalParsingUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.netty.http.client.HttpClient;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Predicate;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Slf4j
@Service
@RequiredArgsConstructor
public class GitHubAccountsProcessingService {

    @Value("${KAFKA_GITHUB_ACCOUNTS_TOPIC}")
    private String accountsTopic;

    @Value("${KAFKA_GITHUB_COMMITS_TOPIC}")
    private String commitsTopic;

    @Value("${CONSUMER_POLLING_TIMEOUT_MS}")
    private Integer consumerPollingTimeout;

    private final HttpClient gitHubHttpClient;
    private final ObjectMapper objectMapper;
    private final List<String> blockedGitHubRepositories;

    @Value("${GITHUB_API_BASE_URL}")
    private String gitHubApiBaseUrl;

    private static final String GITHUB_API_ACCEPT_HEADER = "application/vnd.github.cloak-preview";

    private final static TypeReference<GitHubSearchResponse> gitHubSearchResponseTypeReference = new TypeReference<>() {
    };

    private final KafkaProducerHolder kafkaProducerHolder;
    private final KafkaReceiver<String, GitHubAccount> kafkaReceiver;

    public void process() {
        kafkaReceiver.receive()
            .doOnNext(r -> log.info("Partition: {}, Offset:{}", r.partition(), r.offset()))
            .doOnNext(r -> log.info("Key: {}, Value: {}", r.key(), r.value()))
            .map(ConsumerRecord::value)
            .flatMap(this::getCommits)
            .filter(getGitHubCommitPredicate())
            .doOnNext(this::publishCommit)
            .onErrorContinue((ex, obj) -> log.error("Error while processing account"))
            .doOnComplete(() -> kafkaProducerHolder.getKafkaProducer().flush())
            .subscribe();
    }

    @NotNull
    private Predicate<GitHubCommit> getGitHubCommitPredicate() {
        return commit -> {
            var isBadCommit = blockedGitHubRepositories.contains(commit.getRepositoryFullName());
            if (isBadCommit) {
                log.debug("Skipped commit from blacklisted repository");
                return false;
            } else {
                log.debug("Commit accepted from a trusted repository");
                return true;
            }
        };
    }

    private void publishCommit(GitHubCommit gitHubCommit) {
        log.info("Prepare commit to publish to Kafka '{}'", gitHubCommit);
        var record = new ProducerRecord<>(commitsTopic, gitHubCommit.getSha(), gitHubCommit);
        kafkaProducerHolder.getKafkaProducer().send(record);
    }

    public Flux<GitHubCommit> getCommits(GitHubAccount account) {
        log.info("Start retrieving commits for account '{}'", account.getName());
        return getSearchResponse(account)
            .doOnNext(response -> log.info("Found {} commits for '{}'", response.getItems().length, account.getName()))
            .flatMapMany(gitHubSearchResponse -> Flux.fromArray(gitHubSearchResponse.getItems()))
//            .flatMap(item -> getCommitLanguage(item).map(item::setLanguage))
            .map(GitHubAccountsProcessingService::searchResultItemToCommit);
    }

    private Mono<GitHubSearchResponse> getSearchResponse(GitHubAccount account) {
        return gitHubHttpClient
            .headers(headers -> headers.add("Accept", GITHUB_API_ACCEPT_HEADER))
            .get()
            .uri(buildUri(account))
            .responseSingle((res, byteBufMono) -> {
                var statusCode = res.status().code();
                log.info("Receive {} status code from GitHub for account '{}'", statusCode, account.getName());
                if (statusCode == OK.code()) {
                    return byteBufMono.asString()
                        .map(body -> {
                            try {
                                return objectMapper.readValue(body, gitHubSearchResponseTypeReference);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                } else {
                    log.error("Request failed with status {}", statusCode);
                    return Mono.error(new RuntimeException("Invalid response from GitHub"));
                }
            });
    }

    private String buildUri(GitHubAccount account) {
        var startDateTime = IntervalParsingUtil.getStartDateTime(account.getInterval())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        var searchQuery = String.format("author:%s+author-date:>%s", account.getName(), startDateTime);
        var uri = "%s?q=%s&sort=author-date".formatted(gitHubApiBaseUrl, searchQuery);
        log.debug("[account: {}, interval: {}] GitHub commits URI: '{}'", account.getName(), account.getInterval(), uri);
        return uri;
    }

    private static GitHubCommit searchResultItemToCommit(GitHubSearchResponse.SearchResultItem item) {
        return GitHubCommit.builder()
            .sha(item.getSha())
            .author(item.getAuthor().getLogin())
            .message(item.getCommit().getMessage())
            .dateTimeUtc(item.getCommit().getAuthor().getDate().toLocalDateTime())
            .repositoryFullName(item.getRepository().getFullName())
            .language(item.getLanguage())
            .build();
    }
}
