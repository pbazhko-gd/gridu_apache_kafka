package com.griddynamics.gridu.pbazhko;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;

public class GitHubAccountsSchemaProvider {

    public static Schema getSchema(String topic, String schemaRegistryUrl) {
        var schemaSubject = topic + "-value";
        try (var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10)) {
            var schemaString = schemaRegistryClient.getLatestSchemaMetadata(schemaSubject).getSchema();
            return new Schema.Parser().parse(schemaString);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Cannot detect GitHub accounts schema", e);
        }
    }
}
