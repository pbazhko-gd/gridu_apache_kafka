package com.griddynamics.gridu.pbazhko.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommitersModel {

    private List<CommitterData> committers;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CommitterData {
        private String author;
        private Long commitsCount;
    }

    public static CommitersModel of(CommitterData... committers) {
        return new CommitersModel(Arrays.asList(committers));
    }
}
