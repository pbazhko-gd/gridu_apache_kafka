package com.griddynamics.gridu.pbazhko.util;

import lombok.experimental.UtilityClass;
import org.springframework.util.Assert;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.WEEKS;

@UtilityClass
public class IntervalParsingUtil {

    private static final Map<String, ChronoUnit> UNITS =
        Map.of("h", HOURS, "d", DAYS, "w", WEEKS);

    private static final Pattern DURATION_IN_TEXT_FORMAT = Pattern.compile("^([\\+\\-]?\\d+)([a-zA-Z]{1,2})$");

    public static LocalDateTime getStartDateTime(String interval) {
        var matcher = DURATION_IN_TEXT_FORMAT.matcher(interval);
        Assert.isTrue(matcher.matches(),
            "Interval is malformed, it should be in format <number><unit>, e.g. 3h, 1d, 5w");

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
