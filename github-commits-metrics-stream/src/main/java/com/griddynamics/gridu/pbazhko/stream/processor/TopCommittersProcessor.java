package com.griddynamics.gridu.pbazhko.stream.processor;

import com.griddynamics.gridu.pbazhko.model.CommitersMetricModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Comparator;

@Slf4j
@RequiredArgsConstructor
public class TopCommittersProcessor implements Processor<String, Long, String, CommitersMetricModel> {

    private final String topCommittersStateStoreName;
    private final int topCommittersGroupSize;
    private final String topCommittersKey;

    private KeyValueStore<String, Long> topCommittersStore;
    private ProcessorContext<String, CommitersMetricModel> context;

    @Override
    public void init(ProcessorContext<String, CommitersMetricModel> context) {
        this.context = context;
        this.topCommittersStore = context.getStateStore(topCommittersStateStoreName);
    }

    @Override
    public void process(Record<String, Long> record) {
        var author = record.key();
        var commitsCount = record.value();
        if (author == null || commitsCount == null) {
            return;
        }

        boolean isRatingUpdated = false;
        var previousResultInTop = topCommittersStore.get(author);

        if (previousResultInTop != null) {
            // rewrite the existing result with the new value
            log.debug("Update '{}' in top-{} with {} commit(s)", author, topCommittersGroupSize, commitsCount);
            topCommittersStore.put(author, commitsCount);
            isRatingUpdated = true;
        } else if (getCurrentTopCommittersCount() < topCommittersGroupSize) {
            // put a new author into top if it's size is less than the configured limit
            log.debug("Put '{}' into top-{} with {} commit(s)", author, topCommittersGroupSize, commitsCount);
            topCommittersStore.put(author, commitsCount);
            isRatingUpdated = true;
        } else {
            var currentTopCommitterWithMinCount = getCurrentTopCommitterWithMinCount();
            // rewrite the last user in top with the new author when it's result is better
            if (currentTopCommitterWithMinCount != null && commitsCount > currentTopCommitterWithMinCount.value) {
                log.debug("Replace '{}' in top-{} to '{}' {} commit(s)",
                    currentTopCommitterWithMinCount.key, author, topCommittersGroupSize, commitsCount);
                topCommittersStore.delete(currentTopCommitterWithMinCount.key);
                topCommittersStore.put(author, commitsCount);
                isRatingUpdated = true;
            }
        }

        if (isRatingUpdated) {
            context.forward(new Record<>(topCommittersKey, buildTopCommittersModel(), record.timestamp()));
        }
    }

    private int getCurrentTopCommittersCount() {
        int size = 0;
        try (KeyValueIterator<String, Long> it = topCommittersStore.all()) {
            while (it.hasNext()) {
                it.next();
                size++;
            }
        }
        return size;
    }

    private KeyValue<String, Long> getCurrentTopCommitterWithMinCount() {
        KeyValue<String, Long> min = null;
        try (KeyValueIterator<String, Long> it = topCommittersStore.all()) {
            while (it.hasNext()) {
                KeyValue<String, Long> kv = it.next();
                if (min == null || kv.value < min.value) {
                    min = kv;
                }
            }
        }
        return min;
    }

    private CommitersMetricModel buildTopCommittersModel() {
        var committers = new ArrayList<CommitersMetricModel.CommitterMetricRecord>();

        try (KeyValueIterator<String, Long> it = topCommittersStore.all()) {
            while (it.hasNext()) {
                var kv = it.next();
                committers.add(
                    new CommitersMetricModel.CommitterMetricRecord(kv.key, kv.value)
                );
            }
        }

        committers.sort(
            Comparator
                .comparingLong(CommitersMetricModel.CommitterMetricRecord::getCommitsCount)
                .reversed()
        );

        return new CommitersMetricModel(committers);
    }
}
