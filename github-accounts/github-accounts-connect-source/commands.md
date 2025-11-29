### Get available connector plugins list 
```bash
curl http://localhost:18083/connector-plugins
```

### Get active connectors list 
```bash
curl http://localhost:18083/connectors
```

### Get connector info
```bash
curl http://localhost:18083/connectors/github-accounts-source-connector
```

### Get connector status
```bash
curl http://localhost:18083/connectors/github-accounts-source-connector/status
```

### Delete connector
```bash
curl -X DELETE http://localhost:18083/connectors/github-accounts-source-connector
```

### Register and configure connector
```bash
curl -X POST http://localhost:18083/connectors \
-H "Content-Type: application/json" \
-d '{
    "name": "github-accounts-source-connector",
    "config": {
        "connector.class": "com.griddynamics.gridu.pbazhko.connector.GitHubAccountsSourceConnector",
        "tasks.max": "1",

        "github.accounts.file.path": "/data/github-accounts.txt",
        "github.accounts.topic": "github-accounts",
        
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "key.converter.schemas.enable": "false",

        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.auto.register.schemas": "true",
        "value.converter.schemas.enable": "true",
        "value.converter.schema.registry.url": "http://schema-registry:8081"
    }
}'
```
