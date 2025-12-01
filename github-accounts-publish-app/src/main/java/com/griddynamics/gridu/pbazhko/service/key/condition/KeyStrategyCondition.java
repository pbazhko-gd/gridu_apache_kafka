package com.griddynamics.gridu.pbazhko.service.key.condition;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class KeyStrategyCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        var actual = context.getEnvironment().getProperty("KEY_STRATEGY");
        var attrs = metadata.getAnnotationAttributes(ConditionalOnKeyStrategy.class.getName());
        if (attrs == null) {
            return false;
        }
        var expected = (String) attrs.get("value");
        return expected.equalsIgnoreCase(actual);
    }
}
