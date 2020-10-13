package com.eoi.marayarn.web.base;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.apache.commons.lang.StringUtils;

public interface BaseFilter<T> {
    LambdaQueryWrapper<T> where(LambdaQueryWrapper<T> wrapper);

    //mysql的模糊查询时特殊字符转义
    default String escapeLike(String before){
        if(StringUtils.isNotEmpty(before)){
            before = before.replace("\\", "\\\\");
            before = before.replace("_", "\\_");
            before = before.replace("%", "\\%");
        }
        return StringUtils.isEmpty(before) ? before : "%"+before+"%";
    }
}
