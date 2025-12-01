package com.griddynamics.gridu.pbazhko.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaSchemaRegistryContainer extends GenericContainer<KafkaSchemaRegistryContainer> {

    public KafkaSchemaRegistryContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public String getSchemaRegistry() {
        return "http://" + getHost() + ":" + getFirstMappedPort();
    }
}
