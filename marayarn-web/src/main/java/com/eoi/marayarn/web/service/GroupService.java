package com.eoi.marayarn.web.service;

import com.eoi.marayarn.web.entity.resp.GroupInfoResp;
import com.eoi.marayarn.web.exception.BizException;

/**
 * Created by wenbo.gong on 2020/10/10
 */
public interface GroupService {

    void create(String name,Long parentId) throws BizException;

    void delete(Long id) throws BizException;

    GroupInfoResp listInfo(Long id);
}
