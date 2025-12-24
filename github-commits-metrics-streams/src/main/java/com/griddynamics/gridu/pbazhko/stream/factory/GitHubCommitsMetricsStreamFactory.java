package com.griddynamics.gridu.pbazhko.stream.factory;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

public interface GitHubCommitsMetricsStreamFactory {

    Topology getStreamTopology();

    Properties getStreamProperties();
}
