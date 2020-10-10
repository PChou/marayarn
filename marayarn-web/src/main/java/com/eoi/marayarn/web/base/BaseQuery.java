package com.eoi.marayarn.web.base;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

public abstract class BaseQuery<T> {
    private Integer page;
    private Integer size;

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public LambdaQueryWrapper<T> query() {
        LambdaQueryWrapper<T> wrapper = new LambdaQueryWrapper<>();
        BaseFilter<T> filter = getFilter();
        BaseSort<T> sort = getSort();
        if (filter != null) {
            filter.where(wrapper);
        }
        if (sort != null) {
            sort.order(wrapper);
        }
        return wrapper;
    }

    public Page<T> page() {
        if (page != null && size != null) {
            return new Page<>(page + 1L, size);
        }
        return new Page<>(0, 1000);
    }

    public abstract BaseFilter<T> getFilter();

    public abstract BaseSort<T> getSort();
}
