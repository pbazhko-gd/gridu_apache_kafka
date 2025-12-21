package com.griddynamics.gridu.pbazhko.stream.processor;

import com.griddynamics.gridu.pbazhko.model.LanguagesMetricModel;
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
public class TopLanguagesProcessor implements Processor<String, Long, String, LanguagesMetricModel> {

    private final String topLanguagesStateStoreName;
    private final int topLanguagesGroupSize;
    private final String topLanguagesKey;

    private KeyValueStore<String, Long> topLanguagesStateStore;
    private ProcessorContext<String, LanguagesMetricModel> context;

    @Override
    public void init(ProcessorContext<String, LanguagesMetricModel> context) {
        this.context = context;
        this.topLanguagesStateStore = context.getStateStore(topLanguagesStateStoreName);
    }

    @Override
    public void process(Record<String, Long> record) {
        var language = record.key();
        var commitsCount = record.value();
        if (language == null || commitsCount == null) {
            return;
        }

        boolean isRatingUpdated = false;
        var previousResultInTop = topLanguagesStateStore.get(language);

        if (previousResultInTop != null) {
            // rewrite the existing language in top with the new value
            log.debug("Update '{}' in top-{} with {} commit(s)", language, topLanguagesGroupSize, commitsCount);
            topLanguagesStateStore.put(language, commitsCount);
            isRatingUpdated = true;
        } else if (getCurrentTopLanguagesCount() < topLanguagesGroupSize) {
            // put a new language into top if it's size is less than the configured limit
            log.debug("Put '{}' into top-{} with {} commit(s)", language, topLanguagesGroupSize, commitsCount);
            topLanguagesStateStore.put(language, commitsCount);
            isRatingUpdated = true;
        } else {
            var currentLanguageInTopWithMinResult = getCurrentTopLanguageWithMinCount();
            // rewrite the last language in top with the new language when it's result is better
            if (currentLanguageInTopWithMinResult != null && commitsCount > currentLanguageInTopWithMinResult.value) {
                log.debug("Replace '{}' in top-{} to '{}' {} commit(s)",
                    currentLanguageInTopWithMinResult.key, language, topLanguagesGroupSize, commitsCount);
                topLanguagesStateStore.delete(currentLanguageInTopWithMinResult.key);
                topLanguagesStateStore.put(language, commitsCount);
                isRatingUpdated = true;
            }
        }

        if (isRatingUpdated) {
            context.forward(new Record<>(topLanguagesKey, buildTopCommittersModel(), record.timestamp()));
        }
    }

    private int getCurrentTopLanguagesCount() {
        int size = 0;
        try (KeyValueIterator<String, Long> it = topLanguagesStateStore.all()) {
            while (it.hasNext()) {
                it.next();
                size++;
            }
        }
        return size;
    }

    private KeyValue<String, Long> getCurrentTopLanguageWithMinCount() {
        KeyValue<String, Long> min = null;
        try (KeyValueIterator<String, Long> it = topLanguagesStateStore.all()) {
            while (it.hasNext()) {
                KeyValue<String, Long> kv = it.next();
                if (min == null || kv.value < min.value) {
                    min = kv;
                }
            }
        }
        return min;
    }

    private LanguagesMetricModel buildTopCommittersModel() {
        var languages = new ArrayList<LanguagesMetricModel.LanguageMetricRecord>();

        try (KeyValueIterator<String, Long> it = topLanguagesStateStore.all()) {
            while (it.hasNext()) {
                var kv = it.next();
                languages.add(
                    new LanguagesMetricModel.LanguageMetricRecord(kv.key, kv.value)
                );
            }
        }

        languages.sort(
            Comparator
                .comparingLong(LanguagesMetricModel.LanguageMetricRecord::getCommitsCount)
                .reversed()
        );

        return new LanguagesMetricModel(languages);
    }
}
