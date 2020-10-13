package com.eoi.marayarn.web.entity.db;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by wenbo.gong on 2020/9/29
 */
@TableName("tb_application")
@Getter
@Setter
public class Application {

    @TableId(type = IdType.AUTO)
    private Long id;

    @TableField
    private Long groupId;

    @TableField
    private String absolutePath; //当前目录的绝对路径，必须以'/'结尾

    @TableField
    private String name;

    @TableField
    private String YarnApplicationId;

    @TableField
    private String status;

    @TableField
    private String command;

    @TableField
    private Integer cpu;

    @TableField
    private Integer memory;

    @TableField
    private Integer instanceCount;

    @TableField
    private String constraints;

    @TableField
    private String queue;

    @TableField
    private String user;

    @TableField
    private Long versionTime;
}
