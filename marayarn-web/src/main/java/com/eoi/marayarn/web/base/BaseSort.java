package com.eoi.marayarn.web.base;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.apache.commons.lang.StringUtils;

public interface BaseSort<T> {
    LambdaQueryWrapper<T> order(LambdaQueryWrapper<T> wrapper);

    default boolean isAsc(String s) {
        return !isDesc(s);
    }

    default boolean isDesc(String s) {
        return StringUtils.isNotEmpty(s) && "desc".equalsIgnoreCase(s);
    }
}
