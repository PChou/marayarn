package com.eoi.marayarn.web.controller;

import com.eoi.marayarn.ApplicationInfo;
import com.eoi.marayarn.web.base.ResponseResult;
import com.eoi.marayarn.web.entity.req.AppScaleReq;
import com.eoi.marayarn.web.entity.req.CreateAppReq;
import com.eoi.marayarn.web.exception.BizException;
import com.eoi.marayarn.web.service.ApplicationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Created by wenbo.gong on 2020/10/13
 */
@RestController
@RequestMapping("/api/app")
public class ApplicationController {

    private Logger logger = LoggerFactory.getLogger(ApplicationController.class);

    @Autowired
    private ApplicationService applicationService;

    @PostMapping("")
    public ResponseResult create(@RequestBody CreateAppReq req) {
        try {
            applicationService.create(req);
            return ResponseResult.success();
        } catch (BizException e) {
            return ResponseResult.of(e.getCode(), e.getMessage());
        }
    }

    @PostMapping("/scale/{appId}")
    public ResponseResult scale(@PathVariable Long appId, @RequestBody AppScaleReq req) {
        try {
            applicationService.scale(appId, req);
            return ResponseResult.success();
        } catch (BizException e) {
            return ResponseResult.of(e.getCode(), e.getMessage());
        }
    }


    @DeleteMapping("/destroy/{appId}")
    public ResponseResult destroy(@PathVariable Long appId) {
        try {
            applicationService.destroy(appId);
            return ResponseResult.success();
        } catch (BizException e) {
            return ResponseResult.of(e.getCode(), e.getMessage());
        }
    }

    @GetMapping("/info/{appId}")
    public ResponseResult getAppReport(@PathVariable Long appId) {
        try {
            ApplicationInfo info = applicationService.getAppInfo(appId);
            return ResponseResult.success(info);
        } catch (BizException e) {
            return ResponseResult.of(e.getCode(), e.getMessage());
        }
    }

}
