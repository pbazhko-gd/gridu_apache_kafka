package com.griddynamics.gridu.pbazhko.util;

import java.time.LocalDateTime;
import java.time.ZoneId;

public interface TimeProvider {

    LocalDateTime now(ZoneId zone);
}
