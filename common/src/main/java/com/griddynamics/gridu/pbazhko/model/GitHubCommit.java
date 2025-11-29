package com.griddynamics.gridu.pbazhko.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GitHubCommit {

    private String author;
    private LocalDateTime dateTimeUtc;
    private String language;
    private String sha;
    private String message;
    private String repositoryFullName;
}
