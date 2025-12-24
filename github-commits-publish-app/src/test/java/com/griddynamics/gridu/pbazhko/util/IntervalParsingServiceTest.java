package com.griddynamics.gridu.pbazhko.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IntervalParsingServiceTest {

    @Mock
    private TimeProvider timeProvider;

    @InjectMocks
    private IntervalParsingService service;

    private static final LocalDateTime NOW = LocalDateTime.of(2015, 12, 1, 12, 15, 18);

    @BeforeEach
    void setup() {
        when(timeProvider.now(any())).thenReturn(NOW);
    }

    @Test
    void test_getStartDateTime_hours() {
        var result = service.getStartDateTime("2h");
        assertEquals(NOW.minusHours(2), result);
    }

    @Test
    void test_getStartDateTime_days() {
        var result = service.getStartDateTime("2d");
        assertEquals(NOW.minusDays(2), result);
    }

    @Test
    void test_getStartDateTime_weeks() {
        var result = service.getStartDateTime("2w");
        assertEquals(NOW.minusWeeks(2), result);
    }

    @Test
    void test_getStartDateTime_unknown_unit() {
        var result = assertThrows(IllegalArgumentException.class, () -> service.getStartDateTime("2m"));
        assertEquals("Unknown interval 2m", result.getMessage());
    }
}
