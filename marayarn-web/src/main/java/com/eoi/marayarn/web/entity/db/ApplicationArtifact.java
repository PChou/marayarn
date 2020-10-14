package com.eoi.marayarn.web.entity.db;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by wenbo.gong on 2020/10/13
 */
@TableName("tb_application_artifact")
@Getter
@Setter
public class ApplicationArtifact {

    @TableId
    private Long applicationId;

    @TableId
    private Long artifactId;

    @TableField
    private Long versionTime;

    @TableField
    private String dir;
}
