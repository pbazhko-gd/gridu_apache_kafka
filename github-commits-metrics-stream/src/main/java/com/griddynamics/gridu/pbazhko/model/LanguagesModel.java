package com.griddynamics.gridu.pbazhko.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LanguagesModel {

    private List<LanguageData> languages;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LanguageData {
        private String language;
        private Long commitsCount;
    }

    public static LanguagesModel of(LanguageData... languages) {
        return new LanguagesModel(Arrays.asList(languages));
    }
}
