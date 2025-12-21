package com.griddynamics.gridu.pbazhko.stream;

import lombok.NonNull;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;

public class TestEnvInitializer implements ApplicationContextInitializer<@NonNull ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        var testProps = new HashMap<String, Object>();
        testProps.put("KAFKA_BOOTSTRAP_SERVERS", "dummy:9092");
        testProps.put("TOP_COMMITTERS_GROUP_SIZE", 2);
        testProps.put("TOP_LANGUAGES_GROUP_SIZE", 2);
        applicationContext.getEnvironment()
            .getPropertySources()
            .addFirst(new MapPropertySource("testProps", testProps));
    }
}
