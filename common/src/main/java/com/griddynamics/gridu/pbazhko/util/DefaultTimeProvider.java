package com.griddynamics.gridu.pbazhko.util;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class DefaultTimeProvider implements TimeProvider {

    @Override
    public LocalDateTime now(ZoneId zone) {
        return LocalDateTime.now(zone);
    }
}
