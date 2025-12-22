package com.griddynamics.gridu.pbazhko.stream.processor;

import com.griddynamics.gridu.pbazhko.model.metrics.CommitersMetricModel;
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
public class CommitsPerAuthorProcessor implements Processor<String, Long, String, CommitersMetricModel> {

    private final String commitsCountPerAuthorStateStoreName;
    private final String commitsCountPerAuthorKey;

    private KeyValueStore<String, ValueAndTimestamp<Long>> commitsCountPerAuthorStateStore;
    private ProcessorContext<String, CommitersMetricModel> context;

    @Override
    public void init(ProcessorContext<String, CommitersMetricModel> context) {
        this.context = context;
        this.commitsCountPerAuthorStateStore = context.getStateStore(commitsCountPerAuthorStateStoreName);
    }

    @Override
    public void process(Record<String, Long> record) {
        var model = buildAuthorsMetricModel();
        log.debug("Update commits per author metrics with value '{}'", model);
        context.forward(new Record<>(commitsCountPerAuthorKey, model, record.timestamp()));
    }

    private CommitersMetricModel buildAuthorsMetricModel() {
        var authors = new ArrayList<CommitersMetricModel.CommitterMetricRecord>();

        try (var it = commitsCountPerAuthorStateStore.all()) {
            while (it.hasNext()) {
                var kv = it.next();
                authors.add(new CommitersMetricModel.CommitterMetricRecord(kv.key, kv.value.value()));
            }
        }

        return new CommitersMetricModel(authors);
    }
}
