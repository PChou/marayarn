package com.eoi.marayarn.web.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.eoi.marayarn.web.entity.db.Artifact;
import com.eoi.marayarn.web.entity.req.ArtifactQueryReq;
import com.eoi.marayarn.web.exception.BizException;
import org.springframework.web.multipart.MultipartFile;

import java.net.URL;

/**
 * Created by wenbo.gong on 2020/10/10
 */
public interface ArtifactService {

    void upload(MultipartFile file, String destDir, String fileName) throws BizException;

    void upload(URL url, String destDir, String fileName) throws BizException;

    IPage<Artifact> page(ArtifactQueryReq req);
}
