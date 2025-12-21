package com.griddynamics.gridu.pbazhko.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LanguagesMetricModel {

    private List<LanguageMetricRecord> records;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LanguageMetricRecord {
        private String language;
        private Long commitsCount;
    }

    public static LanguagesMetricModel of(LanguageMetricRecord... languageMetricRecords) {
        return new LanguagesMetricModel(Arrays.asList(languageMetricRecords));
    }
}
