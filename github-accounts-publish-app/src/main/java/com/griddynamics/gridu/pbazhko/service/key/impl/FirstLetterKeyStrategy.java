package com.griddynamics.gridu.pbazhko.service.key.impl;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import com.griddynamics.gridu.pbazhko.service.key.KafkaKeyStrategy;
import com.griddynamics.gridu.pbazhko.service.key.condition.ConditionalOnKeyStrategy;
import com.griddynamics.gridu.pbazhko.service.key.condition.KeyStrategy;
import com.griddynamics.gridu.pbazhko.service.key.condition.KeyStrategyCondition;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

@Service
@Conditional(KeyStrategyCondition.class)
@ConditionalOnKeyStrategy(KeyStrategy.FIRST_LETTER)
public class FirstLetterKeyStrategy implements KafkaKeyStrategy {

    @Override
    public String getKey(GitHubAccount account) {
        return String.valueOf(account.getName().toLowerCase().charAt(0));
    }
}
