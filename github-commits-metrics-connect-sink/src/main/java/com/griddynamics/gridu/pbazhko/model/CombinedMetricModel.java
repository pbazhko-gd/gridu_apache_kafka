package com.griddynamics.gridu.pbazhko.model;

import com.griddynamics.gridu.pbazhko.model.metrics.CommitersMetricModel;
import com.griddynamics.gridu.pbazhko.model.metrics.LanguagesMetricModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CombinedMetricModel {
    private LanguagesMetricModel topLanguages;
    private LanguagesMetricModel commitsPerLanguage;
    private CommitersMetricModel topCommitters;
    private CommitersMetricModel commitsPerAuthor;
    private Long totalCommitsCount;
    private Long totalCommittersCount;
}
