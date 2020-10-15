package com.eoi.marayarn.web.service;

import com.eoi.marayarn.ApplicationInfo;
import com.eoi.marayarn.web.entity.req.AppScaleReq;
import com.eoi.marayarn.web.entity.req.CreateAppReq;
import com.eoi.marayarn.web.exception.BizException;

/**
 * Created by wenbo.gong on 2020/9/29
 */
public interface ApplicationService {

    void create(CreateAppReq req) throws BizException;

    void scale(Long appId, AppScaleReq req) throws BizException;

    void destroy(Long appId) throws BizException;

    ApplicationInfo getAppInfo(Long applicationId) throws BizException;
}
