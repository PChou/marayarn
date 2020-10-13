package com.eoi.marayarn.web.entity.req;

import com.eoi.marayarn.web.base.BaseQuery;
import com.eoi.marayarn.web.entity.db.Artifact;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by wenbo.gong on 2020/10/10
 */

@Getter
@Setter
public class ArtifactQueryReq extends BaseQuery<Artifact> {

    private ArtifactFilterReq filter;

    private ArtifactSortReq sort;

}
