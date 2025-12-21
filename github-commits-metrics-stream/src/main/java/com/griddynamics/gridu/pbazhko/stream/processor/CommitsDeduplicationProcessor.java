package com.griddynamics.gridu.pbazhko.stream.processor;

import com.griddynamics.gridu.pbazhko.model.GitHubCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
@RequiredArgsConstructor
public class CommitsDeduplicationProcessor implements Processor<String, GitHubCommit, String, GitHubCommit> {

    private final String uniqueShaStateStore;

    private KeyValueStore<String, Boolean> store;
    private ProcessorContext<String, GitHubCommit> context;

    @Override
    public void init(ProcessorContext<String, GitHubCommit> context) {
        this.context = context;
        store = context.getStateStore(uniqueShaStateStore);
    }

    @Override
    public void process(Record<String, GitHubCommit> record) {
        var sha = record.value().getSha();
        if (store.get(sha) == null) {
            store.put(sha, true);
            context.forward(record);
        } else {
            log.debug("Skip duplicate commit with SHA '{}'", sha);
        }
    }
}
