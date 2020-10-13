package com.eoi.marayarn.web.entity.req;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eoi.marayarn.web.base.BaseFilter;
import com.eoi.marayarn.web.entity.db.Artifact;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@Setter
@Getter
public class ArtifactFilterReq implements BaseFilter<Artifact> {

    private String name;
    private String directory;

    private Long timeFrom;
    private Long timeTo;

    @Override
    public LambdaQueryWrapper<Artifact> where(LambdaQueryWrapper<Artifact> wrapper) {
        return wrapper.like(StringUtils.isNotEmpty(name),Artifact::getName,escapeLike(name))
                .likeRight(StringUtils.isNotEmpty(directory),Artifact::getDirectory,escapeLike(directory))
                .ge(timeFrom != null,Artifact::getCreateTime,timeFrom)
                .le(timeTo != null,Artifact::getCreateTime,timeTo);
    }
}
