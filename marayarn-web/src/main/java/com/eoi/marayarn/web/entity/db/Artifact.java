package com.eoi.marayarn.web.entity.db;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@Getter
@Setter
@TableName("tb_artifact")
public class Artifact {

    @TableId(type = IdType.AUTO)
    private Long id;

    @TableField
    private String name;

    @TableField
    private String hdfsAddr;

    @TableField
    private String directory;

    @TableField
    private Long createTime;

}
