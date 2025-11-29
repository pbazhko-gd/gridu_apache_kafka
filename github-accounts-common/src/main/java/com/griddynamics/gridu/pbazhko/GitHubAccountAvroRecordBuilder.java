package com.griddynamics.gridu.pbazhko;

import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class GitHubAccountAvroRecordBuilder {

    public static GenericRecord build(Schema avroSchema, GitHubAccount account) {
        var record = new GenericData.Record(avroSchema);

        record.put("name", account.getName());
        record.put("interval", account.getInterval());

        if (!GenericData.get().validate(avroSchema, record)) {
            throw new RuntimeException("Check schema for GitHub account");
        }

        return record;
    }
}
