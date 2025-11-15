package com.griddynamics.gridu.pbazhko.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class GitHubCommit {

    @JsonProperty(required = true)
    private String author;

    @JsonProperty(required = true)
    private LocalDateTime dateTimeUtc;

    @JsonProperty(required = true)
    private String language;

    @JsonProperty(required = true)
    private String sha;

    @JsonProperty(required = true)
    private String message;

    @JsonProperty(required = true)
    private String repositoryFullName;
}
