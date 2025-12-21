package com.griddynamics.gridu.pbazhko.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommitersMetricModel {

    private List<CommitterMetricRecord> records;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CommitterMetricRecord {
        private String author;
        private Long commitsCount;
    }

    public static CommitersMetricModel of(CommitterMetricRecord... committerMetricRecord) {
        return new CommitersMetricModel(Arrays.asList(committerMetricRecord));
    }
}
