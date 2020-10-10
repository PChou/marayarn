package com.eoi.marayarn.web.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.eoi.marayarn.web.entity.db.Artifact;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@Repository
@Mapper
public interface ArtifactMapper extends BaseMapper<Artifact> {
}
