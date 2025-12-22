package com.griddynamics.gridu.pbazhko.stream.processor;

import com.griddynamics.gridu.pbazhko.model.metrics.LanguagesMetricModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;

@Slf4j
@RequiredArgsConstructor
public class CommitsPerLanguageProcessor implements Processor<String, Long, String, LanguagesMetricModel> {

    private final String commitsCountPerLanguageStateStoreName;
    private final String commitsCountPerLanguageKey;

    private KeyValueStore<String, ValueAndTimestamp<Long>> commitsCountPerLanguageStateStore;
    private ProcessorContext<String, LanguagesMetricModel> context;

    @Override
    public void init(ProcessorContext<String, LanguagesMetricModel> context) {
        this.context = context;
        this.commitsCountPerLanguageStateStore = context.getStateStore(commitsCountPerLanguageStateStoreName);
    }

    @Override
    public void process(Record<String, Long> record) {
        var model = buildLanguagesMetricModel();
        log.debug("Update commits per language metrics with value '{}'", model);
        context.forward(new Record<>(commitsCountPerLanguageKey, model, record.timestamp()));
    }

    private LanguagesMetricModel buildLanguagesMetricModel() {
        var languages = new ArrayList<LanguagesMetricModel.LanguageMetricRecord>();

        try (var it = commitsCountPerLanguageStateStore.all()) {
            while (it.hasNext()) {
                var kv = it.next();
                languages.add(new LanguagesMetricModel.LanguageMetricRecord(kv.key, kv.value.value()));
            }
        }

        return new LanguagesMetricModel(languages);
    }
}
