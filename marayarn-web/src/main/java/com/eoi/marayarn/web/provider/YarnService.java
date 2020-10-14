package com.eoi.marayarn.web.provider;

import com.eoi.marayarn.*;
import com.eoi.marayarn.web.entity.db.Application;
import com.eoi.marayarn.web.entity.req.AppScaleReq;
import com.eoi.marayarn.web.enums.MsgCode;
import com.eoi.marayarn.web.exception.BizException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Created by wenbo.gong on 2020/10/14
 */
@Service
public class YarnService implements InitializingBean {

    private Logger logger = LoggerFactory.getLogger(YarnService.class);

    @Value("${hadoop.conf.dir}")
    private String hadoopConfDir;

    @Value("${hadoop.yarn.amJar}")
    private String yarnAmJar;

    @Value("${hadoop.yarn.amClz}")
    private String yarnAmClz;

    private volatile boolean isInit = false;

    private AMClient amClient;

    private YarnClient yarnClient;


    public ApplicationReport getAppReport(String applicationId) throws Exception {
        ensureInit();
        return yarnClient.getApplicationReport(ApplicationId.fromString(applicationId));
    }


    public AMResponse scale(String applicationId, AppScaleReq req) throws Exception {
        ensureInit();

        ScaleRequest request = new ScaleRequest();
        request.setInstances(req.getCount());
        request.setContainerIds(req.getKillContainerIds());

        ApplicationReport report = getAppReport(applicationId);
        switch (report.getYarnApplicationState()) {
            case FINISHED:
            case FAILED:
            case KILLED:
                throw new BizException(MsgCode.STATUS_ERROR);
        }
        logger.info("Scale application id: {} ,tracking url: {} ,instance :{}, containerId :{}",
                report.getApplicationId(), report.getTrackingUrl(), request.getInstances(), request.getContainerIds());

        return amClient.scaleApplication(report.getOriginalTrackingUrl(), request);
    }

    public AMResponse stop(String applicationId) throws Exception{
        ensureInit();
        ApplicationReport report = getAppReport(applicationId);
        switch (report.getYarnApplicationState()) {
            case FINISHED:
            case FAILED:
            case KILLED:
                AMResponse resp = new AMResponse();
                resp.setCode("0");
                resp.setMessage("OK");
                return resp;
        }
       return amClient.stopApplication(report.getOriginalTrackingUrl());
    }

    public ApplicationReport submitApplication(Application app, List<Artifact> artifactList, Map<String, String> env) throws Exception {
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationName(app.getName());
        arguments.setApplicationMasterJar(yarnAmJar);
        arguments.setApplicationMasterClass(yarnAmClz);
        arguments.setHadoopConfDir(hadoopConfDir);
        arguments.setCpu(app.getCpu());
        arguments.setMemory(app.getMemory());
        arguments.setInstances(app.getInstanceCount());
        arguments.setCommand(app.getCommand());
        arguments.setConstraints(app.getConstraints());
        arguments.setExecutorEnvironments(env);
        arguments.setQueue(app.getQueue());
        arguments.setUser(app.getUser());
        arguments.setArtifacts(artifactList);

        return submitApplication(arguments);
    }

    public ApplicationReport submitApplication(ClientArguments arguments) throws Exception {
        ensureInit();
        try (Client client = new Client()) {
            ApplicationReport report = client.launch(arguments);
            logger.info("Submit application id: {} ,tracking url: {}", report.getApplicationId(), report.getTrackingUrl());
            return report;
        }
    }

    public ApplicationInfo getAppInfo(String applicationId) throws Exception {
        ApplicationReport report = getAppReport(applicationId);
        switch (report.getYarnApplicationState()) {
            case FINISHED:
            case FAILED:
            case KILLED:
                throw new BizException(MsgCode.STATUS_ERROR);
        }
        return amClient.getApplication(report.getOriginalTrackingUrl());
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        ensureInit();
    }

    private void ensureInit() throws Exception {
        if (!isInit) {
            synchronized (this) {
                if (!isInit) {
                    logger.info("Try to init AmClient and YarnClient...");
                    amClient = new AMClient();
                    yarnClient = YarnClient.createYarnClient();
                    YarnConfiguration yarnConfiguration = new YarnConfiguration();
                    yarnConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

                    File[] files = FileUtil.listFiles(new File(hadoopConfDir));
                    for (File f : files) {
                        if (f.isFile() && f.canRead() && f.getName().endsWith(".xml")) {
                            yarnConfiguration.addResource(new Path(f.getAbsolutePath()));
                        }
                    }
                    yarnClient.init(yarnConfiguration);
                    yarnClient.start();
                    isInit = true;
                    logger.info("Init AmClient and YarnClient successfully");
                }
            }
        }
    }
}
