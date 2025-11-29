package com.griddynamics.gridu.pbazhko.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GitHubAccount {

    @JsonProperty(required = true)
    private String name;

    @JsonProperty(required = true)
    private String interval;

    public static GitHubAccount fromCsvLine(String csv) {
        var data = csv.trim().split(",");
        if (data.length != 2) {
            throw new RuntimeException("Cannot parse GitHub account CSV line '%s'".formatted(csv));
        }
        return new GitHubAccount(data[0], data[1]);
    }
}
