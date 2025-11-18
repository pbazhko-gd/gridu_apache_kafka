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
}
