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
@TableName("tb_group")
@Getter
@Setter
public class Group {
    @TableId(type = IdType.AUTO)
    private Long id;

    @TableField
    private String name;

    @TableField
    private Long parentId;

    @TableField
    private String absolutePath; //绝对路径,以'/'结尾

}
