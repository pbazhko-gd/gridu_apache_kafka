package com.griddynamics.gridu.pbazhko.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopLanguagesModel {

    private List<LanguageModel> committers;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LanguageModel {
        private String language;
        private Long commitsCount;
    }

    public static TopLanguagesModel of(LanguageModel... languages) {
        return new TopLanguagesModel(Arrays.asList(languages));
    }
}
