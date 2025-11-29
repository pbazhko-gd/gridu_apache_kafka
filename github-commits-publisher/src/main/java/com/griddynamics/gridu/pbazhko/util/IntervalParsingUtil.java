package com.griddynamics.gridu.pbazhko.util;

import lombok.experimental.UtilityClass;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.WEEKS;

import static com.griddynamics.gridu.pbazhko.model.GitHubAccount.INTERVAL_PATTERN;

@UtilityClass
public class IntervalParsingUtil {

    private static final Map<String, ChronoUnit> UNITS =
        Map.of("h", HOURS, "d", DAYS, "w", WEEKS);

    public static LocalDateTime getStartDateTime(String interval) {
        var matcher = INTERVAL_PATTERN.matcher(interval);

        var amount = Long.parseLong(matcher.group(1));
        var unit = UNITS.get(matcher.group(2).toLowerCase());

        return switch (unit) {
            case HOURS -> LocalDateTime.now(ZoneOffset.UTC).minusHours(amount);
            case DAYS -> LocalDateTime.now(ZoneOffset.UTC).minusDays(amount);
            case WEEKS -> LocalDateTime.now(ZoneOffset.UTC).minusWeeks(amount);
            default -> throw new IllegalArgumentException("Unknown time unit " + unit);
        };
    }
}
