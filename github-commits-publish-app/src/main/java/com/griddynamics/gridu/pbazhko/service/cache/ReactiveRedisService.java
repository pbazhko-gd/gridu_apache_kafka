package com.griddynamics.gridu.pbazhko.service.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Mono;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReactiveRedisService {

    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper;

    public <T> Mono<Void> set(String key, T value, Duration ttl) {
        return Mono.fromCallable(() -> {
            var json = objectMapper.writeValueAsString(value);
            try (var jedis = jedisPool.getResource()) {
                log.trace("Set cache key '{}'", key);
                jedis.set(key, json, SetParams.setParams().ex(ttl.getSeconds()));
            }
            return (Void) null;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public <T> Mono<T> get(String key, Class<T> clazz) {
        return Mono.fromCallable(() -> {
            try (var jedis = jedisPool.getResource()) {
                log.trace("Get cache by key '{}'", key);
                var json = jedis.get(key);
                if (json == null) return null;
                return objectMapper.readValue(json, clazz);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
