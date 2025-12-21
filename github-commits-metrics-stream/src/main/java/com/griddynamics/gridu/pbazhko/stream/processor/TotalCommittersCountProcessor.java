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
public class TotalCommittersCountProcessor implements Processor<String, GitHubCommit, String, Long> {

    private final String totalCommittersCountStateStore;
    private final String totalCommittersCountKey;

    private ProcessorContext<String, Long> context;
    private KeyValueStore<String, Long> uniqueCommittersStore;

    @Override
    public void init(ProcessorContext<String, Long> context) {
        this.context = context;
        this.uniqueCommittersStore = context.getStateStore(totalCommittersCountStateStore);
    }

    @Override
    public void process(Record<String, GitHubCommit> record) {
        var committer = record.value().getAuthor();
        if (uniqueCommittersStore.get(committer) == null) {
            log.debug("New committer detected: '{}'", committer);
            uniqueCommittersStore.put(committer, 1L);

            long total = uniqueCommittersStore.approximateNumEntries();

            context.forward(new Record<>(totalCommittersCountKey, total, record.timestamp()));
        } else {
            log.debug("Skip duplicate committer: '{}'", committer);
        }
    }
}
