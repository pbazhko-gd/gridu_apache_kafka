package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
@Service
public class GitHubAccountsReadingService {

    public List<GitHubAccount> readAll(String filename) {
        try (var lines = getLinesStream(filename)) {
            return lines
                .map(this::convertToModel)
                .filter(Objects::nonNull)
                .distinct() // avoid potential duplicates in file
                .toList();
        } catch (Exception e) {
            log.error("Cannot read file {}", filename, e);
            throw new RuntimeException(e);
        }
    }

    private Stream<String> getLinesStream(String filename) throws IOException {
        var path = Path.of(filename);
        if (Files.exists(path)) {
            return Files.lines(path);
        }

        var in = getClass().getClassLoader().getResourceAsStream(filename);
        if (in != null) {
            var reader = new BufferedReader(new InputStreamReader(in));
            return reader.lines().onClose(() -> {
                try {
                    reader.close();
                } catch (IOException ignored) {
                }
            });
        }

        throw new IllegalArgumentException("File not found");
    }

    private GitHubAccount convertToModel(String line) {
        try {
            return GitHubAccount.fromCsvLine(line);
        } catch (Exception e) {
            log.error("Skip invalid line '{}'", line, e);
            return null;
        }
    }
}
