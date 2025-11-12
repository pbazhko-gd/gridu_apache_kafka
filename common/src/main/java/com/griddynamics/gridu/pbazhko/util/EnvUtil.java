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
    private static final Properties PROPERTIES_FROM_CONFIG_FILE = loadProperties();

    private static Properties loadProperties() {
        var props = new Properties();

        try (var inputStream = EnvUtil.class
            .getClassLoader()
            .getResourceAsStream(PROPERTIES_FILENAME)) {

            if (inputStream != null) {
                log.info("Read properties from '{}' file", PROPERTIES_FILENAME);
                props.load(inputStream);
            } else {
                log.warn("'{}' file was not found", PROPERTIES_FILENAME);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read properties file '%s'".formatted(PROPERTIES_FILENAME), e);
        }

        return props;
    }

    public static String getConfig(String key) {
        return Optional.ofNullable(
            System.getenv().getOrDefault(key, PROPERTIES_FROM_CONFIG_FILE.getProperty(key))
        ).orElseThrow(() -> new RuntimeException("Property key '%s' not found".formatted(key)));
    }
}
