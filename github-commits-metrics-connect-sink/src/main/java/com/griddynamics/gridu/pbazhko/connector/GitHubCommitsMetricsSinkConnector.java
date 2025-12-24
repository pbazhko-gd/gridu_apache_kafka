package com.griddynamics.gridu.pbazhko.connector;

import com.griddynamics.gridu.pbazhko.task.OverwriteFileSinkTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.*;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Slf4j
public class GitHubCommitsMetricsSinkConnector extends SinkConnector {

    public static final String TARGET_FILE_PATH = "target.file.path";
    public static final String TOP_COMMITTERS_TOPIC_NAME = "top.committers.topic.name";
    public static final String TOP_LANGUAGES_TOPIC_NAME = "top.languages.topic.name";
    public static final String COMMITS_PER_AUTHOR_TOPIC_NAME = "commits.per.author.topic.name";
    public static final String COMMITS_PER_LANGUAGE_TOPIC_NAME = "commits.per.language.topic.name";
    public static final String TOTAL_COMMITS_COUNT_TOPIC_NAME = "total.commits.count.topic.name";
    public static final String TOTAL_COMMITTERS_COUNT_TOPIC_NAME = "total.committers.count.topic.name";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TARGET_FILE_PATH, STRING, null, HIGH, "Path to the target file")
        .define(TOP_COMMITTERS_TOPIC_NAME, STRING, null, HIGH, "Top committers topic name")
        .define(TOP_LANGUAGES_TOPIC_NAME, STRING, null, HIGH, "Top languages topic name")
        .define(COMMITS_PER_AUTHOR_TOPIC_NAME, STRING, null, HIGH, "Commits per author topic name")
        .define(COMMITS_PER_LANGUAGE_TOPIC_NAME, STRING, null, HIGH, "Commits per language topic name")
        .define(TOTAL_COMMITS_COUNT_TOPIC_NAME, STRING, null, HIGH, "Total commits count topic name")
        .define(TOTAL_COMMITTERS_COUNT_TOPIC_NAME, STRING, null, HIGH, "Total committers count topic name");

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
        log.info("GitHub commits metrics sink connector started");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OverwriteFileSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        var configs = new ArrayList<Map<String, String>>();
        configs.add(new HashMap<>(configProps));
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
