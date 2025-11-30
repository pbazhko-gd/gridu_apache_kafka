package com.griddynamics.gridu.pbazhko.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.regex.Pattern;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GitHubAccount {

    @JsonProperty(required = true)
    private String name;

    @JsonProperty(required = true)
    private String interval;

    public static final Pattern INTERVAL_PATTERN = Pattern.compile("^(\\d+)([hdw])$");

    public static GitHubAccount fromCsvLine(String csv) {
        var data = csv.trim().split(",");
        if (data.length != 2) {
            throw new IllegalArgumentException("Cannot parse GitHub account CSV line '%s'".formatted(csv));
        }
        var name = data[0].trim();
        var interval = data[1].trim();
        var matcher = INTERVAL_PATTERN.matcher(interval);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Interval is malformed, it should be in format <number><unit>, e.g. 3h, 1d, 5w");
        }
        return new GitHubAccount(name, interval);
    }
}
