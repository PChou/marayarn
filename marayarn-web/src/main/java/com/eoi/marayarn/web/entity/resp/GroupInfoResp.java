package com.eoi.marayarn.web.entity.resp;

import lombok.*;

import java.util.List;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@Setter
@Getter
public class GroupInfoResp {

    private List<Item> groupInfos;

    private List<Item> appInfos;

    @Getter
    @Setter
    @NoArgsConstructor
    @Builder
    public static class Item {
        private Long id;
        private String name;
        private Integer cpu;
        private Integer memory;
        private String status;
        private Integer runningInstance;
        private Integer totalInstance;
    }
}
