package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
public class GitHubAccountsReadingService {

    @Value("${GITHUB_ACCOUNTS_LIST_FILENAME}")
    private String filename;

    public List<GitHubAccount> readAll() {
        try {
            return Files.readAllLines(Paths.get(filename))
                .stream()
                .map(this::convertToModel)
                .filter(Objects::nonNull)
                .distinct() // avoid potential duplicates in file
                .toList();
        } catch (IOException e) {
            log.error("Cannot read file {}", filename, e);
            throw new RuntimeException(e);
        }
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
