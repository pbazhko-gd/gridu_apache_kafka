package com.griddynamics.gridu.pbazhko.service;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GitHubAccountsReadingServiceTest {

    private final GitHubAccountsReadingService gitHubAccountsReadingService =
        new GitHubAccountsReadingService();

    @Test
    void test_readAll_2_accounts_configured() {
        var result = gitHubAccountsReadingService.readAll("test-github-accounts.txt");
        var expectedAccount1 = new GitHubAccount("user1", "1d");
        var expectedAccount2 = new GitHubAccount("user2", "2w");
        assertEquals(2, result.size());
        assertEquals(expectedAccount1, result.get(0));
        assertEquals(expectedAccount2, result.get(1));
    }
}
