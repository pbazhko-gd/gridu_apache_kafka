package com.griddynamics.gridu.pbazhko.connector;

import com.griddynamics.gridu.pbazhko.task.GitHubAccountsTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Slf4j
public class GitHubAccountsSourceConnector extends SourceConnector {

    public static final String GITHUB_ACCOUNTS_FILE_PATH = "github.accounts.file.path";
    public static final String GITHUB_ACCOUNTS_TOPIC = "github.accounts.topic";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(SCHEMA_REGISTRY_URL, STRING, null, HIGH, "Schema registry URL")
        .define(GITHUB_ACCOUNTS_FILE_PATH, STRING, null, HIGH, "Path to the file with the GitHub accounts list")
        .define(GITHUB_ACCOUNTS_TOPIC, STRING, null, HIGH, "Target Kafka topic to push GitHub accounts");

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
        log.info("GitHub accounts source connector started");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GitHubAccountsTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        var configs = new ArrayList<Map<String, String>>();
        configs.add(configProps);
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
