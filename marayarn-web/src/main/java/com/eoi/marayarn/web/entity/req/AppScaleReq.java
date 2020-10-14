package com.eoi.marayarn.web.entity.req;

import lombok.Data;

import java.util.List;

/**
 * Created by wenbo.gong on 2020/10/14
 */
@Data
public class AppScaleReq {
    private Integer count;
    private List<String> killContainerIds;
}
