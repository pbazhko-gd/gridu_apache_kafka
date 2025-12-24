package com.griddynamics.gridu.pbazhko.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.griddynamics.gridu.pbazhko.connector.GitHubCommitsMetricsSinkConnector;
import com.griddynamics.gridu.pbazhko.model.CombinedMetricModel;
import com.griddynamics.gridu.pbazhko.model.metrics.CommitersMetricModel;
import com.griddynamics.gridu.pbazhko.model.metrics.LanguagesMetricModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Map;

import static com.griddynamics.gridu.pbazhko.connector.GitHubCommitsMetricsSinkConnector.*;

@Slf4j
public class OverwriteFileSinkTask extends SinkTask {

    private Path filePath;

    private String topCommittersTopic;
    private String topLanguagesTopic;
    private String commitsPerLanguageTopic;
    private String commitsPerAuthorTopic;
    private String totalCommitsCountTopic;
    private String totalCommittersCountTopic;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<CombinedMetricModel> COMBINED_METRIC_MODEL_TYPE_REFERENCE =
        new TypeReference<>() {};
    private static final TypeReference<LanguagesMetricModel> LANGUAGES_METRIC_MODEL_TYPE_REFERENCE =
        new TypeReference<>() {};
    private static final TypeReference<CommitersMetricModel> COMMITERS_METRIC_MODEL_TYPE_REFERENCE =
        new TypeReference<>() {};

    @Override
    public void start(Map<String, String> props) {
        this.filePath = Path.of(props.get(TARGET_FILE_PATH));
        this.topCommittersTopic = props.get(TOP_COMMITTERS_TOPIC_NAME);
        this.topLanguagesTopic = props.get(TOP_LANGUAGES_TOPIC_NAME);
        this.commitsPerLanguageTopic = props.get(COMMITS_PER_LANGUAGE_TOPIC_NAME);
        this.commitsPerAuthorTopic = props.get(COMMITS_PER_AUTHOR_TOPIC_NAME);
        this.totalCommitsCountTopic = props.get(TOTAL_COMMITS_COUNT_TOPIC_NAME);
        this.totalCommittersCountTopic = props.get(TOTAL_COMMITTERS_COUNT_TOPIC_NAME);
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Receive {} sink record(s)", records.size());
        if (records.isEmpty()) {
            return;
        }
        try {
            var existingCombinedMetric = getExistingCombinedMetric();
            var hasChanges = records.stream()
                .map(record -> {
                    try {
                        return updateCombinedMetricModel(record, existingCombinedMetric);
                    } catch (JsonProcessingException e) {
                        return false;
                    }
                })
                .toList()
                .stream()
                .anyMatch(Boolean.TRUE::equals);
            if (hasChanges) {
                var content = OBJECT_MAPPER.writeValueAsString(existingCombinedMetric);
                var tmpFile = Path.of(filePath + ".tmp");
                Files.writeString(tmpFile, content, StandardCharsets.UTF_8);
                Files.move(tmpFile, filePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } else {
                log.info("No changes in existing metrics");
            }
        } catch (IOException e) {
            log.error("Failed to process incoming event", e);
        }
    }

    private boolean updateCombinedMetricModel(SinkRecord record, CombinedMetricModel existingCombinedMetric)
        throws JsonProcessingException {
        var topic = record.topic();
        if (topic.equals(topCommittersTopic)) {
            log.info("Update top committers metric");
            existingCombinedMetric.setTopCommitters(parseMetric(record, COMMITERS_METRIC_MODEL_TYPE_REFERENCE));
            return true;
        }
        if (topic.equals(topLanguagesTopic)) {
            log.info("Update top languages metric");
            existingCombinedMetric.setTopLanguages(parseMetric(record, LANGUAGES_METRIC_MODEL_TYPE_REFERENCE));
            return true;
        }
        if (topic.equals(commitsPerAuthorTopic)) {
            log.info("Update commits per author metric");
            existingCombinedMetric.setCommitsPerAuthor(parseMetric(record, COMMITERS_METRIC_MODEL_TYPE_REFERENCE));
            return true;
        }
        if (topic.equals(commitsPerLanguageTopic)) {
            log.info("Update commits per language metric");
            existingCombinedMetric.setCommitsPerLanguage(parseMetric(record, LANGUAGES_METRIC_MODEL_TYPE_REFERENCE));
            return true;
        }
        if (topic.equals(totalCommitsCountTopic)) {
            log.info("Update total commits count metric");
            existingCombinedMetric.setTotalCommitsCount((Long) record.value());
            return true;
        }
        if (topic.equals(totalCommittersCountTopic)) {
            log.info("Update total committers count metric");
            existingCombinedMetric.setTotalCommittersCount((Long) record.value());
            return true;
        }
        log.error("Unsupported topic '{}", topic);
        return false;
    }

    private <T> T parseMetric(Object value, TypeReference<T> typeReference) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(value.toString(), typeReference);
    }

    private CombinedMetricModel getExistingCombinedMetric() throws IOException {
        if (filePath.toFile().exists()) {
            return OBJECT_MAPPER.readValue(filePath.toFile(), COMBINED_METRIC_MODEL_TYPE_REFERENCE);
        } else {
            return new CombinedMetricModel();
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public String version() {
        return new GitHubCommitsMetricsSinkConnector().version();
    }
}
