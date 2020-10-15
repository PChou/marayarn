package com.eoi.marayarn.web.entity.req;

import lombok.Data;

import java.util.Map;

/**
 * Created by wenbo.gong on 2020/10/13
 */
@Data
public class CreateAppReq {

    private String name;

    private Long groupId;

    private Integer cpu;

    private Integer memory;

    private Integer instanceCount;

    private String command;

    private Map<Long,String> artifacts; //ID -> 解压目录

    private Map<String,String> env;

    private String constraints;

    private String user;

    private String queue;
}
