package com.griddynamics.gridu.pbazhko.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class GitHubSearchResponse {

    @JsonProperty("total_count")
    private int totalCount;

    private SearchResultItem[] items;

    @Data
    public static class SearchResultItem {
        private String sha;
        private CommitInfo commit;
        private AuthorInfo author;
        private RepositoryInfo repository;

        @JsonIgnore
        private String language;
    }

    @Data
    public static class CommitInfo {
        private String message;
        private CommitAuthorInfo author;
    }

    @Data
    public static class CommitAuthorInfo {
        private ZonedDateTime date;
    }

    @Data
    public static class AuthorInfo {
        private String login;
    }

    @Data
    public static class RepositoryInfo {

        @JsonProperty("full_name")
        private String fullName;

        @JsonProperty("languages_url")
        private String languagesUrl;
    }
}
