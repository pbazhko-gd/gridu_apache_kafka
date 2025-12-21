package com.griddynamics.gridu.pbazhko.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Properties;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommonUtils {

    public static Properties mergeProperties(Properties... properties) {
        var props = new Properties();
        Arrays.asList(properties).forEach(props::putAll);
        return props;
    }
}
