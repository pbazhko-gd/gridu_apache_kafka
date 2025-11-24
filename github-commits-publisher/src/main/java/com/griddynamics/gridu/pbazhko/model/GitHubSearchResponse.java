package com.griddynamics.gridu.pbazhko.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class GitHubSearchResponse {

    @JsonProperty("total_count")
    private int totalCount;

    private SearchResultItem[] items;

    @JsonProperty("incomplete_results")
    private boolean incompleteResults;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SearchResultItem {
        private String sha;
        private CommitInfo commit;
        private AuthorInfo author;
        private RepositoryInfo repository;

        @JsonIgnore
        private String language;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CommitInfo {
        private String message;
        private CommitAuthorInfo author;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CommitAuthorInfo {
        private ZonedDateTime date;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AuthorInfo {
        private String login;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RepositoryInfo {

        @JsonProperty("full_name")
        private String fullName;

        @JsonProperty("languages_url")
        private String languagesUrl;
    }
}
