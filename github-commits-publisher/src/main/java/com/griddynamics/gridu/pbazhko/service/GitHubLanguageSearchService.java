package com.griddynamics.gridu.pbazhko.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.griddynamics.gridu.pbazhko.service.cache.ReactiveRedisService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Slf4j
@Service
@RequiredArgsConstructor
public class GitHubLanguageSearchService {

    private final HttpClient gitHubHttpClient;
    private final ObjectMapper objectMapper;
    private final ReactiveRedisService reactiveRedisService;

    @Value("${LANGUAGES_CACHE_ENABLED}")
    private boolean languagesCacheEnabled;

    private final static TypeReference<Map<String, Long>> gitHubRepositorySearchResponseTypeReference =
        new TypeReference<>() {};

    public Mono<String> getCommitLanguage(String languagesUrl) {
        if (languagesCacheEnabled) {
            return reactiveRedisService.get(languagesUrl, String.class)
                .doOnNext(response ->
                    log.debug("Found cached language '{}' for url '{}'", response, languagesUrl)
                ).switchIfEmpty(Mono.defer(() -> {
                    log.debug("No cached language for url '{}'", languagesUrl);
                    return retrieveCommitLanguage(languagesUrl)
                        .flatMap(lang -> {
                            log.debug("Cache language '{}' for url '{}'", lang, languagesUrl);
                            return reactiveRedisService.set(languagesUrl, lang, Duration.ofDays(1))
                                .thenReturn(lang);
                        });
                }));
        } else {
            return retrieveCommitLanguage(languagesUrl);
        }
    }

    private Mono<String> retrieveCommitLanguage(String languagesUrl) {
        return gitHubHttpClient.get()
            .uri(languagesUrl)
            .responseSingle((res, byteBufMono) -> {
                var statusCode = res.status().code();
                log.trace("Receive {} status code from GitHub Repository API for repository '{}'", statusCode, languagesUrl);
                if (statusCode == OK.code()) {
                    return byteBufMono.asString()
                        .map(body -> {
                            try {
                                return objectMapper.readValue(body, gitHubRepositorySearchResponseTypeReference);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                } else {
                    log.error("Request failed with status {}", statusCode);
                    return Mono.just(new HashMap<String, Long>());
                }
            })
            .onErrorResume(e -> Mono.just(new HashMap<>()))
            .map(languages -> {
                if (languages.isEmpty()) {
                    return "Undefined";
                }
                var entries = new ArrayList<>(languages.entrySet());
                entries.sort(Map.Entry.comparingByValue());
                return entries.getLast().getKey();
            });
    }
}
