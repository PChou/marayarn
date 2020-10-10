package com.eoi.marayarn.web.service.impl;

import com.eoi.marayarn.web.entity.db.Artifact;
import com.eoi.marayarn.web.enums.MsgCode;
import com.eoi.marayarn.web.exception.BizException;
import com.eoi.marayarn.web.mapper.ArtifactMapper;
import com.eoi.marayarn.web.service.ArtifactService;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@Service
public class ArtifactServiceImpl implements ArtifactService {

    @Value("${hdfs.conf.dir}")
    private String hdfsDir;

    private Logger logger = LoggerFactory.getLogger(ArtifactServiceImpl.class);

    @Autowired
    private ArtifactMapper artifactMapper;

    @Override
    public void upload(MultipartFile file, String destDir, String fileName) throws BizException {
        if (!file.isEmpty()) {
            String destName = StringUtils.isEmpty(fileName) ? file.getOriginalFilename() : fileName;
            if (StringUtils.isEmpty(destName)) {
                throw new BizException(MsgCode.INVALID_PARAM);
            }
            try {
                upload(file.getInputStream(), destDir, fileName);
            } catch (IOException e) {
                logger.error("Upload artifact error", e);
                throw new BizException(MsgCode.SYSTEM_ERROR, e);
            }
        }
    }

    @Override
    public void upload(URL url, String destDir, String fileName) throws BizException {
//        System.setProperty("file.encoding", "GBK"); //ftp资源带有中文时可能需要该设置
        if (StringUtils.isEmpty(fileName)) {
            fileName = url.getFile().substring(url.getFile().lastIndexOf("/") + 1);
        }
        try {
            upload(url.openStream(), destDir, fileName);
        } catch (IOException e) {
            logger.error("Upload artifact error", e);
            throw new BizException(MsgCode.SYSTEM_ERROR, e);
        }
    }

    private void upload(InputStream in, String destDir, String fileName) throws BizException {
        try (FileSystem fs = getHDFS()) {
            Path parentDir = StringUtils.isEmpty(destDir) ? fs.getHomeDirectory() : new Path(destDir);
            OutputStream os = fs.create(new Path(parentDir, fileName));
            IOUtils.copyBytes(in, os, 4096, true);

            //保存记录
            Artifact artifact = new Artifact();
            artifact.setName(fileName);
            artifact.setDirectory(parentDir.toString());
            artifact.setCreateTime(System.currentTimeMillis());
            artifact.setHdfsAddr(fs.getUri().toString());
            artifactMapper.insert(artifact);
        } catch (IOException e) {
            logger.error("Upload file to HDFS error!", e);
            throw new BizException(MsgCode.SYSTEM_ERROR, e);
        }
    }


    private FileSystem getHDFS() throws BizException {
        try {
            Configuration conf = new Configuration();
            File[] files = FileUtil.listFiles(new File(hdfsDir));
            for (File f : files) {
                if (f.isFile() && f.canRead() && f.getName().endsWith(".xml")) {
                    conf.addResource(new Path(f.getAbsolutePath()));
                }
            }
            return FileSystem.get(conf);
        } catch (Exception e) {
            logger.error("Get HDFS error!", e);
            throw new BizException(MsgCode.SYSTEM_ERROR, e);
        }
    }
}
