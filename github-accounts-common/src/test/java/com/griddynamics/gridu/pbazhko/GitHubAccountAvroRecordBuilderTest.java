package com.griddynamics.gridu.pbazhko;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GitHubAccountAvroRecordBuilderTest {

    private static final GitHubAccount GITHUB_ACCOUNT =
        new GitHubAccount("user", "1d");

    @Test
    void testBuild_schema_no_match() {
        var schema = SchemaBuilder
            .record(GitHubAccount.class.getName())
            .fields()
            .requiredString("id")
            .requiredString("name")
            .requiredString("interval")
            .endRecord();
        var exception = assertThrows(
            RuntimeException.class,
            () -> GitHubAccountAvroRecordBuilder.build(schema, GITHUB_ACCOUNT)
        );
        assertEquals("Check schema for GitHub account", exception.getMessage());
    }

    @Test
    void testBuild_schema_match() {
        var schema = SchemaBuilder
            .record(GitHubAccount.class.getName())
            .fields()
            .requiredString("name")
            .requiredString("interval")
            .endRecord();
        var result = GitHubAccountAvroRecordBuilder.build(schema, GITHUB_ACCOUNT);
        assertEquals("user", result.get("name"));
        assertEquals("1d", result.get("interval"));
    }
}
