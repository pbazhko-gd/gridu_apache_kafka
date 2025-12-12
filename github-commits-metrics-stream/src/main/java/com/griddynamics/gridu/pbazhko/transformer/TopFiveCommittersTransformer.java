package com.griddynamics.gridu.pbazhko.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopFiveCommittersTransformer implements Transformer<String, Long, KeyValue<String, String>> {

    private final String storeName;
    private KeyValueStore<String, ValueAndTimestamp> store;

    public TopFiveCommittersTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore) context.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, String> transform(String s, Long aLong) {
        KeyValueIterator<String, ValueAndTimestamp> iterator = store.all();
        List<KeyValue<String, ValueAndTimestamp>> authors = new ArrayList<>();

        while (iterator.hasNext()) {
            authors.add(iterator.next());
        }

        authors.sort(Comparator.comparing(o ->
            ((KeyValue<String, ValueAndTimestamp<Long>>) o).value.value()).reversed());

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5 && i < authors.size(); i++) {
            sb.append(authors.get(i).key)
                .append(" (").append(authors.get(i).value.value()).append(")");
            if (i != 4 && i != authors.size() - 1) {
                sb.append(", ");
            }
        }

        return new KeyValue<>("top5-committers", "top5_committers: " + sb.toString());
    }

    @Override
    public void close() {

    }
}
