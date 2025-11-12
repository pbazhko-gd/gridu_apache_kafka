package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.util.EnvUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class GitHubAccountsProvidingService {

    @SneakyThrows
    public List<GitHubAccount> findAll() {
        var filename = EnvUtil.getConfig("GITHUB_ACCOUNTS_LIST_FILENAME");
        return Files.readAllLines(Paths.get(filename))
            .stream()
            .map(String::trim)
            .map(GitHubAccount::new)
            .toList();
    }
}
