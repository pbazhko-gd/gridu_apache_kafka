package com.griddynamics.gridu.pbazhko.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.griddynamics.gridu.pbazhko.config.KafkaProducerHolder;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import com.griddynamics.gridu.pbazhko.model.GitHubCommitsSearchResponse;
import com.griddynamics.gridu.pbazhko.service.cache.ReactiveRedisService;
import com.griddynamics.gridu.pbazhko.util.IntervalParsingUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Slf4j
@Service
@RequiredArgsConstructor
public class GitHubAccountsProcessingService {

    @Value("${KAFKA_GITHUB_COMMITS_TOPIC}")
    private String commitsTopic;

    @Value("${COMMITS_CACHE_ENABLED}")
    private boolean commitsCacheEnabled;

    private final HttpClient gitHubHttpClient;
    private final ObjectMapper objectMapper;
    private final ReactiveRedisService reactiveRedisService;
    private final GitHubLanguageSearchService languageSearchService;
    private final List<String> blockedGitHubRepositories;

    @Value("${GITHUB_API_BASE_URL}")
    private String gitHubApiBaseUrl;

    private static final String GITHUB_API_ACCEPT_HEADER = "application/vnd.github.cloak-preview";

    private final static TypeReference<GitHubCommitsSearchResponse> gitHubCommitsSearchResponseTypeReference =
        new TypeReference<>() {};

    private final KafkaProducerHolder kafkaProducerHolder;
    private final KafkaReceiver<String, GitHubAccount> kafkaReceiver;

    public void process() {
        kafkaReceiver
            .receive()
            .flatMap(record ->
                process(record)
                    .then(record.receiverOffset().commit())
                    .onErrorResume(ex -> {
                        log.error("Error committing offset for account '{}'", record.value(), ex);
                        return Mono.empty();
                    })
            ).subscribe();
    }

    private Flux<GitHubCommit> process(ReceiverRecord<String, GitHubAccount> r) {
        var account = r.value();
        log.debug("Receive new message from Kafka. Partition: {}, Offset:{}, Key: {}, Value: {}",
            r.partition(), r.offset(), r.key(), r.value());
        log.info("Start processing account '{}'", account);

        return getCommits(account)
            .doOnNext(this::publishCommit)
            .doOnComplete(() -> kafkaProducerHolder.getKafkaProducer().flush())
            .onErrorResume(ex -> {
                log.error("Error processing account '{}'", account, ex);
                return Flux.empty();
            });
    }

    private void publishCommit(GitHubCommit gitHubCommit) {
        log.info("Prepare commit to publish to Kafka '{}'", gitHubCommit);
        var record = new ProducerRecord<>(commitsTopic, gitHubCommit.getSha(), gitHubCommit);
        kafkaProducerHolder.getKafkaProducer().send(record);
    }

    public Flux<GitHubCommit> getCommits(GitHubAccount account) {
        log.trace("Start retrieving commits for account '{}'", account);
        return getCacheableCommitsSearchResponse(account)
            .doOnNext(response -> log.info("Found {} commits for '{}'", response.getItems().length, account.getName()))
            .flatMapMany(gitHubCommitsSearchResponse -> Flux.fromArray(gitHubCommitsSearchResponse.getItems()))
            .filter(this::isRepositoryTrusted)
            .flatMap(item -> languageSearchService.getCommitLanguage(item.getRepository().getLanguagesUrl())
                .map(l -> {
                    item.setLanguage(l);
                    return item;
                }))
            .map(GitHubAccountsProcessingService::searchResultItemToCommit);
    }

    private boolean isRepositoryTrusted(GitHubCommitsSearchResponse.SearchResultItem item) {
        var repositoryFullName = item.getRepository().getFullName();
        var isBadCommit = blockedGitHubRepositories.contains(repositoryFullName);
        if (isBadCommit) {
            log.debug("Skipped commit from blacklisted repository '{}'", repositoryFullName);
            return false;
        } else {
            log.debug("Commit accepted from a trusted repository '{}'", repositoryFullName);
            return true;
        }
    }

    private Mono<GitHubCommitsSearchResponse> getCacheableCommitsSearchResponse(GitHubAccount account) {
        if (commitsCacheEnabled) {
            return reactiveRedisService.get(buildCacheKey(account), GitHubCommitsSearchResponse.class)
                .publishOn(Schedulers.immediate())
                .doOnNext(response ->
                    log.debug("Found cached GitHub commit response for account '{}'", account)
                ).switchIfEmpty(Mono.defer(() -> {
                    log.debug("No cached GitHub commit response for account '{}'", account);
                    return getCommitsSearchResponse(account)
                        .flatMap(resp -> {
                            log.debug("Cache GitHub commit response for account '{}'", account);
                            return reactiveRedisService.set(buildCacheKey(account), resp, Duration.ofDays(1))
                                .thenReturn(resp);
                        });
                }));
        } else {
            return getCommitsSearchResponse(account);
        }
    }

    private Mono<GitHubCommitsSearchResponse> getCommitsSearchResponse(GitHubAccount account) {
        return gitHubHttpClient
            .headers(headers -> headers.add("Accept", GITHUB_API_ACCEPT_HEADER))
            .get()
            .uri(buildCommitsUri(account))
            .responseSingle((res, byteBufMono) -> {
                var statusCode = res.status().code();
                log.trace("Receive {} status code from GitHub Commits API for account '{}'", statusCode, account);
                if (statusCode == OK.code()) {
                    return byteBufMono.asString()
                        .map(body -> {
                            try {
                                return objectMapper.readValue(body, gitHubCommitsSearchResponseTypeReference);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                } else {
                    log.error("Request failed with status {}", statusCode);
                    throw new RuntimeException("Invalid response from GitHub");
                }
            });
    }

    private String buildCommitsUri(GitHubAccount account) {
        var startDateTime = IntervalParsingUtil.getStartDateTime(account.getInterval())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        var searchQuery = String.format("author:%s+author-date:>%s", account.getName(), startDateTime);
        var uri = "%s?q=%s&sort=author-date".formatted(gitHubApiBaseUrl, searchQuery);
        log.trace("GitHub commits URI '{}' for account: '{}', interval: '{}'", uri, account.getName(), account.getInterval());
        return uri;
    }

    private static GitHubCommit searchResultItemToCommit(GitHubCommitsSearchResponse.SearchResultItem item) {
        return GitHubCommit.builder()
            .sha(item.getSha())
            .author(item.getAuthor().getLogin())
            .message(item.getCommit().getMessage())
            .dateTimeUtc(item.getCommit().getAuthor().getDate().toLocalDateTime())
            .repositoryFullName(item.getRepository().getFullName())
            .language(item.getLanguage())
            .build();
    }

    private String buildCacheKey(GitHubAccount account) {
        return "%s|%s".formatted(account.getName(), account.getInterval());
    }
}
