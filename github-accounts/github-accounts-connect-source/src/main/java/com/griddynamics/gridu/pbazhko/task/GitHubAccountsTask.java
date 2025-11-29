package com.griddynamics.gridu.pbazhko.task;

import com.griddynamics.gridu.pbazhko.connector.GitHubAccountsSourceConnector;
import com.griddynamics.gridu.pbazhko.model.GitHubAccount;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class GitHubAccountsTask extends SourceTask {

    private static final int BATCH_SIZE = 10;
    private static final long TASK_DELAY_MS = 1000;

    private String topic;
    private String filePath;
    private BufferedReader reader;
    private long offset = 0;

    private static final Schema GITHUB_ACCOUNT_SCHEMA =
        SchemaBuilder.struct()
            .name(GitHubAccount.class.getName())
            .field("name", Schema.STRING_SCHEMA)
            .field("interval", Schema.STRING_SCHEMA)
            .build();

    @Override
    public String version() {
        return new GitHubAccountsSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {

        filePath = props.get(GitHubAccountsSourceConnector.GITHUB_ACCOUNTS_FILE_PATH);
        topic = props.get(GitHubAccountsSourceConnector.GITHUB_ACCOUNTS_TOPIC);

        log.info("Start GitHub accounts source connector task with props '{}'", props);

        try {
            reader = new BufferedReader(new FileReader(filePath));
            var savedOffset = context.offsetStorageReader()
                .offset(Collections.singletonMap("file", filePath));
            if (savedOffset != null) {
                long savedPos = (Long) savedOffset.get("position");
                reader.skip(savedPos);
                offset = savedPos;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        var records = new ArrayList<SourceRecord>();
        try {
            String line;
            while ((line = reader.readLine()) != null) {

                offset += line.getBytes().length + 1; // + newline

                var gitHubAccount = parseGitHubAccount(line);
                if (gitHubAccount == null) {
                    log.info("Skip invalid line '{}'", line);
                    continue;
                } else {
                    log.info("Read new GitHub account '{}'", gitHubAccount);
                }

                var key = String.valueOf(gitHubAccount.getName().charAt(0));

                var sourceRecord = new SourceRecord(
                    Collections.singletonMap("file", filePath),
                    Collections.singletonMap("position", offset),
                    topic,
                    null,
                    null,
                    key,
                    GITHUB_ACCOUNT_SCHEMA,
                    toStruct(gitHubAccount)
                );
                records.add(sourceRecord);

                if (records.size() >= BATCH_SIZE) {
                    break; // batch
                }
            }
            if (records.isEmpty()) {
                Thread.sleep(TASK_DELAY_MS);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return records;
    }

    @Override
    public void stop() {
        try {
            reader.close();
        } catch (Exception ex) {
            log.error("Error occurred while closing buffered reader", ex);
        }
    }

    private GitHubAccount parseGitHubAccount(String line) {
        try {
            return GitHubAccount.fromCsvLine(line);
        } catch (Exception e) {
            log.error("Invalid line '{}'", line, e);
            return null;
        }
    }

    private static Struct toStruct(GitHubAccount account) {
        return new Struct(GITHUB_ACCOUNT_SCHEMA)
            .put("name", account.getName())
            .put("interval", account.getInterval());
    }
}
