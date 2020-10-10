package com.eoi.marayarn.web.controller;

import com.eoi.marayarn.web.base.ResponseResult;
import com.eoi.marayarn.web.entity.req.UploadReq;
import com.eoi.marayarn.web.enums.MsgCode;
import com.eoi.marayarn.web.exception.BizException;
import com.eoi.marayarn.web.service.ArtifactService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@RestController
@RequestMapping("/api/artifact")
public class ArtifactController {

    private Logger logger = LoggerFactory.getLogger(ArtifactController.class);

    @Autowired
    private ArtifactService artifactService;


    @PutMapping("/upload")
    public ResponseResult upload(@RequestParam("file") MultipartFile file,
                                 @RequestParam String destDir,
                                 @RequestParam(required = false) String fileName) {
        try {
            artifactService.upload(file, destDir, fileName);
            return ResponseResult.success();
        } catch (BizException e) {
            logger.error("Upload error!", e);
            return ResponseResult.of(e.getCode(), e.getMessage());
        }
    }

    @PutMapping("/transfer")
    public ResponseResult transfer(@RequestBody UploadReq req) {
        try {
            artifactService.upload(new URL(req.getUrl()), req.getDestDir(), req.getFileName());
            return ResponseResult.success();
        } catch (BizException e) {
            logger.error("Upload error!", e);
            return ResponseResult.of(e.getCode(), e.getMessage());
        } catch (MalformedURLException e) {
            logger.error("Parse URL error", e);
            return ResponseResult.of(MsgCode.INVALID_PARAM.code, "URL解析异常");
        }
    }
}
