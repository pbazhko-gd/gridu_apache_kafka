package com.griddynamics.gridu.pbazhko.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopCommitersModel {

    private List<CommitterModel> committers;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CommitterModel {
        private String author;
        private Long commitsCount;
    }

    public static TopCommitersModel of(CommitterModel... committers) {
        return new TopCommitersModel(Arrays.asList(committers));
    }
}
