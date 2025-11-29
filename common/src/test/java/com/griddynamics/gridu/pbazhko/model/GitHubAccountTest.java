package com.griddynamics.gridu.pbazhko.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GitHubAccountTest {

    @Test
    void testFromCsvLine_invalid_line() {
        var exception = assertThrows(IllegalArgumentException.class, () -> GitHubAccount.fromCsvLine("user,"));
        assertEquals("Cannot parse GitHub account CSV line 'user,'", exception.getMessage());
    }

    @Test
    void testFromCsvLine_invalid_interval() {
        var exception = assertThrows(IllegalArgumentException.class, () -> GitHubAccount.fromCsvLine("user,12y"));
        assertEquals("Interval is malformed, it should be in format <number><unit>, e.g. 3h, 1d, 5w", exception.getMessage());
    }

    @Test
    void testFromCsvLine_valid_interval() {
        var result = GitHubAccount.fromCsvLine("user,1d");
        assertEquals(new GitHubAccount("user", "1d"), result);
    }
}
