package com.griddynamics.gridu.pbazhko.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import com.griddynamics.gridu.pbazhko.model.GitHubSearchResponse;
import com.griddynamics.gridu.pbazhko.util.IntervalParsingUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.format.DateTimeFormatter;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Slf4j
@Service
@RequiredArgsConstructor
public class GitHubCommitsRetrievingService {

    private final HttpClient gitHubHttpClient;
    private final ObjectMapper objectMapper;

    @Value("${GITHUB_API_BASE_URL}")
    private String gitHubApiBaseUrl;

    private static final String GITHUB_API_ACCEPT_HEADER = "application/vnd.github.cloak-preview";

    private final static TypeReference<GitHubSearchResponse> gitHubSearchResponseTypeReference = new TypeReference<>() {
    };

    public Flux<GitHubCommit> getCommits(GitHubAccount account) {
        log.info("Start retrieving commits for account '{}'", account.getName());
        var response = getSearchResponse(account);
        return response.flatMapMany(gitHubSearchResponse -> Flux.fromArray(gitHubSearchResponse.getItems()))
//            .flatMap(item -> getCommitLanguage(item).map(item::setLanguage))
            .map(GitHubCommitsRetrievingService::searchResultItemToCommit);
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
                                log.info("Response body: {}", body);
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
        return "%s?q=%s&sort=author-date".formatted(gitHubApiBaseUrl, searchQuery);
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
