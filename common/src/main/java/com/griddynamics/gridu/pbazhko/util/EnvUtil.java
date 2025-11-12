package com.griddynamics.gridu.pbazhko.util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

@Slf4j
@UtilityClass
public class EnvUtil {

    private static final String PROPERTIES_FILENAME = "application.properties";
    private static final Properties PROPERTIES = loadProperties();

    private static Properties loadProperties() {
        var props = new Properties();

        try (var inputStream = EnvUtil.class
            .getClassLoader()
            .getResourceAsStream(PROPERTIES_FILENAME)) {

            if (inputStream != null) {
                props.load(inputStream);
                props.forEach((key, value) -> log.info("Found configuration property {}={}", key, value));
            } else {
                log.warn("'{}' file not found", PROPERTIES_FILENAME);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read properties file '%s'".formatted(PROPERTIES_FILENAME), e);
        }

        System.getenv().forEach((key, value) -> {
            if (value != null && !value.isEmpty()) {
                props.setProperty(key, value);
            }
        });

        return props;
    }

    public static String getConfig(String key) {
        return Optional.ofNullable(PROPERTIES.getProperty(key))
            .orElseThrow(() -> new RuntimeException("Property key '%s' not found".formatted(key)));
    }
}
