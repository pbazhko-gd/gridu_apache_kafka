package com.griddynamics.gridu.pbazhko.topology;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

public interface GitHubCommitsMetricsTopology {

    Topology getTopology();

    Properties getCustomProperties();
}
