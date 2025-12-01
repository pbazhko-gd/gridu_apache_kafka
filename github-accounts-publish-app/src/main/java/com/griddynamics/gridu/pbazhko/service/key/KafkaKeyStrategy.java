package com.griddynamics.gridu.pbazhko.service.key;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;

public interface KafkaKeyStrategy {

    String getKey(GitHubAccount account);
}
