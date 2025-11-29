### How to install connector
1. Execute **mvn clean package**
2. Move jar file to **docker/kafka_connect_plugins**
3. Restart all kafka-connect containers
4. Use the command listed below to operate with the connector
5. GitHub accounts list is located in **docker/github-accounts.txt** file (check docker-compose.yml)

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
