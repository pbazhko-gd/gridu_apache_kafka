package com.griddynamics.gridu.pbazhko.util;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.WEEKS;

import static com.griddynamics.gridu.pbazhko.model.GitHubAccount.INTERVAL_PATTERN;

@Component
@RequiredArgsConstructor
public class IntervalParsingService {

    private final TimeProvider timeProvider;

    private static final Map<String, ChronoUnit> UNITS =
        Map.of("h", HOURS, "d", DAYS, "w", WEEKS);

    public LocalDateTime getStartDateTime(String interval) {
        var matcher = INTERVAL_PATTERN.matcher(interval);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Unknown interval " + interval);
        }

        var amount = Long.parseLong(matcher.group(1));
        var unit = UNITS.get(matcher.group(2).toLowerCase());

        return switch (unit) {
            case HOURS -> timeProvider.now(ZoneOffset.UTC).minusHours(amount);
            case DAYS -> timeProvider.now(ZoneOffset.UTC).minusDays(amount);
            case WEEKS -> timeProvider.now(ZoneOffset.UTC).minusWeeks(amount);
            default -> throw new IllegalArgumentException("Unknown time unit " + unit);
        };
    }
}
