package com.eoi.marayarn.web.entity.req;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eoi.marayarn.web.base.BaseSort;
import com.eoi.marayarn.web.entity.db.Artifact;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@Getter
@Setter
public class ArtifactSortReq implements BaseSort<Artifact> {

    private String name;
    private String createTime;

    @Override
    public LambdaQueryWrapper<Artifact> order(LambdaQueryWrapper<Artifact> wrapper) {
        return wrapper.orderBy(StringUtils.isNotEmpty(name),isAsc(name),Artifact::getName)
                .orderBy(StringUtils.isNotEmpty(createTime),isAsc(createTime),Artifact::getId);
        //id 和 createTime 都是升序，而id有主键索引，因此直接对主键排序
    }
}
