package com.eoi.marayarn.web.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eoi.marayarn.AMResponse;
import com.eoi.marayarn.ApplicationInfo;
import com.eoi.marayarn.JsonUtil;
import com.eoi.marayarn.web.entity.db.Application;
import com.eoi.marayarn.web.entity.db.ApplicationArtifact;
import com.eoi.marayarn.web.entity.db.Artifact;
import com.eoi.marayarn.web.entity.db.Group;
import com.eoi.marayarn.web.entity.req.AppScaleReq;
import com.eoi.marayarn.web.entity.req.CreateAppReq;
import com.eoi.marayarn.web.enums.ApplicationStatus;
import com.eoi.marayarn.web.enums.MsgCode;
import com.eoi.marayarn.web.exception.BizException;
import com.eoi.marayarn.web.mapper.ApplicationMapper;
import com.eoi.marayarn.web.mapper.ArtifactMapper;
import com.eoi.marayarn.web.mapper.GroupMapper;
import com.eoi.marayarn.web.provider.YarnService;
import com.eoi.marayarn.web.service.ApplicationArtifactService;
import com.eoi.marayarn.web.service.ApplicationService;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wenbo.gong on 2020/9/29
 */
@Service
public class ApplicationServiceImpl implements ApplicationService, InitializingBean {

    private Logger logger = LoggerFactory.getLogger(ApplicationServiceImpl.class);

    @Autowired
    private ApplicationMapper applicationMapper;
    @Autowired
    private GroupMapper groupMapper;
    @Autowired
    private ArtifactMapper artifactMapper;
    @Autowired
    private ApplicationArtifactService applicationArtifactService;
    @Autowired
    private YarnService yarnService;

    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();


    //防止afterPropertiesSet启动的检测线程被错误的多次调用
    private volatile boolean checkThreadRunning = false;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!checkThreadRunning) {
            synchronized (this) {
                if (!checkThreadRunning) {
                    new Thread(() -> {
                        LambdaQueryWrapper<Application> wrapper = new LambdaQueryWrapper<>();
                        wrapper.in(Application::getStatus, Arrays.asList(ApplicationStatus.SCALING.value, ApplicationStatus.DESTROYING.value));
                        while (true) {
                            try {
                                List<Application> list = applicationMapper.selectList(wrapper);
                                if (!CollectionUtils.isEmpty(list)) {
                                    for (Application app : list) {
                                        try {
                                            if (ApplicationStatus.DESTROYING.value.equals(app.getStatus())) {
                                                //检测删除
                                                ApplicationReport report = yarnService.getAppReport(app.getYarnApplicationId());
                                                switch (report.getYarnApplicationState()) {
                                                    case FINISHED:
                                                    case KILLED:
                                                    case FAILED:
                                                        //删除应用
                                                        delete(app.getId());
                                                }
                                            } else {
                                                ApplicationInfo info = yarnService.getAppInfo(app.getYarnApplicationId());
                                                if (info.getNumRunningExecutors() == info.getNumTotalExecutors()) {
                                                    //scale完成
                                                    if (info.getNumTotalExecutors() == 0) {
                                                        //suspend
                                                        app.setStatus(ApplicationStatus.SUSPEND.value);
                                                        app.setInstanceCount(0);
                                                    } else {
                                                        app.setStatus(ApplicationStatus.RUNNING.value);
                                                    }
                                                    applicationMapper.updateById(app);
                                                }
                                            }
                                        } catch (Exception e) {
                                            logger.error("Check application [" + app.getId() + "][" + app.getName() + "][" + app.getYarnApplicationId() + "] error", e);
                                        }
                                    }
                                }
                                Thread.sleep(3000);
                            } catch (Exception e) {
                                logger.error("Check application task error", e);
                            }
                        }

                    }, "app-status-check").start();

                    checkThreadRunning = true;
                }
            }
        }

    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void create(CreateAppReq req) throws BizException {
        ensure(req);

        //创建应用
        Application app = insert(req);

        //建立artifact关联
        Map<Long, String> dirMap = req.getArtifacts(); //artifact id -> 解压目录
        Map<Long, Artifact> artifactMap = saveArtifacts(app, dirMap);

        //构建提交所需artifact信息
        List<com.eoi.marayarn.Artifact> clientArtifacts = new ArrayList<>();
        for (Map.Entry<Long, Artifact> entry : artifactMap.entrySet()) {
            com.eoi.marayarn.Artifact clientArtifact = new com.eoi.marayarn.Artifact();
            clientArtifact.setType(LocalResourceType.FILE);

            Artifact artifact = entry.getValue();
            String dir = dirMap.get(entry.getKey());
            StringBuilder path = new StringBuilder();
            path.append(artifact.getDirectory()).append("/").append(artifact.getName());
            if (!StringUtils.isEmpty(dir)) {
                path.append("#").append(dir);
                if (artifact.getName().endsWith(".tar.gz") || artifact.getName().endsWith(".zip")) {
                    clientArtifact.setType(LocalResourceType.ARCHIVE);
                }
            }
            clientArtifact.setLocalPath(path.toString());
            clientArtifacts.add(clientArtifact);
        }

        //todo 添加历史配置信息

        //异步提交任务
        submitApp(app, clientArtifacts, req.getEnv());
    }


    @Override
    public void scale(Long appId, AppScaleReq req) throws BizException {
        Application application = applicationMapper.selectById(appId);
        if (application == null) {
            throw new BizException(MsgCode.NOT_EXIST);
        }
        String yarnAppId = application.getYarnApplicationId();
        if (!StringUtils.isEmpty(yarnAppId)) {
            //状态判断
            if (ApplicationStatus.READY.value.equals(application.getStatus())
                    || ApplicationStatus.DESTROYING.value.equals(application.getStatus())) {
                throw new BizException(MsgCode.STATUS_ERROR.code, "处于就绪、删除状态的应用无法进行该操作");
            }

            //scale
            try {
                AMResponse resp = yarnService.scale(yarnAppId, req);
                if (resp != null && resp.getCode().equals("0")) {
                    //扩容成功,但需要异步确认
                    application.setInstanceCount(req.getCount());
                    application.setStatus(ApplicationStatus.SCALING.value);
                    applicationMapper.updateById(application);
                } else if (resp != null) {
                    logger.warn("Scale application [{}][{}] failed, response code = {}, msg = {}", appId, yarnAppId, resp.getCode(), resp.getMessage());
                    throw new BizException(MsgCode.REQUEST_FAILED);
                } else {
                    logger.warn("Scale application [{}][{}] failed, response is null", appId, yarnAppId);
                    throw new BizException(MsgCode.REQUEST_FAILED);
                }
            } catch (Exception e) {
                logger.error("Scale application [" + appId + "][" + yarnAppId + "] error", e);
                throw new BizException(MsgCode.SYSTEM_ERROR, e);
            }
        }
    }

    @Override
    public void destroy(Long appId) throws BizException {
        Application application = applicationMapper.selectById(appId);
        if (application != null) {
            if (ApplicationStatus.FAILED.value.equals(application.getStatus())) {
                //失败状态可以直接删除
                delete(appId);
            } else if (ApplicationStatus.READY.value.equals(application.getStatus())) {
                //就绪状态正在提交应用，未防止异步操作导致yarn资源无法回收，该状态下禁止删除
                throw new BizException(MsgCode.STATUS_ERROR.code, "该状态无法执行删除操作");
            } else if (!ApplicationStatus.DESTROYING.value.equals(application.getStatus())) {
                String yarnAppId = application.getYarnApplicationId();
                try {
                    AMResponse resp = yarnService.stop(yarnAppId);
                    if ("0".equals(resp.getCode())) {
                        //等待异步确认
                        application.setStatus(ApplicationStatus.DESTROYING.value);
                        applicationMapper.updateById(application);
                    } else {
                        logger.warn("Stop application [{}][{}] failed, response code = {}, msg = {}", appId, yarnAppId, resp.getCode(), resp.getMessage());
                        throw new BizException(MsgCode.REQUEST_FAILED);
                    }
                } catch (Exception e) {
                    logger.error(String.format("Stop application [%d][%s] error", appId, yarnAppId), e);
                    throw new BizException(MsgCode.SYSTEM_ERROR, e);
                }
            }
        }
    }


    /**
     * 调用该方法前需先判断yarn上是否还在运行
     *
     * @param appId
     */
    private void delete(Long appId) {
        applicationMapper.deleteById(appId);
        applicationArtifactService.remove(new LambdaQueryWrapper<ApplicationArtifact>().eq(ApplicationArtifact::getApplicationId, appId));
        //TODO 删除发布信息
        //todo 删除历史配置
    }

    @Override
    public ApplicationInfo getAppInfo(Long applicationId) throws BizException {
        Application application = applicationMapper.selectById(applicationId);
        if (application == null) {
            throw new BizException(MsgCode.NOT_EXIST);
        }
        if (StringUtils.isEmpty(application.getYarnApplicationId())) {
            return null;
        }
        try {
            return yarnService.getAppInfo(application.getYarnApplicationId());
        } catch (Exception e) {
            logger.error("Get application info for[" + application.getYarnApplicationId() + "] error", e);
            if (e instanceof BizException) {
                throw (BizException) e;
            } else {
                throw new BizException(MsgCode.SYSTEM_ERROR, e);
            }
        }
    }


    private void submitApp(Application app, List<com.eoi.marayarn.Artifact> artifactList, Map<String, String> env) {
        executorService.submit(() -> {
            try {
                logger.info("Starting submit application[{}][{}]", app.getId(), app.getName());
                ApplicationReport report = yarnService.submitApplication(app, artifactList, env);

                //提交完成更新状态
                app.setYarnApplicationId(report.getApplicationId().toString());
                app.setStatus(ApplicationStatus.SCALING.value);
                applicationMapper.updateById(app);
                logger.info("Finish submit application[{}][{}] ,yarnApplicationId : {}", app.getId(), app.getName(), app.getYarnApplicationId());
                //TODO 添加版本发布信息
            } catch (Exception e) {
                logger.error(String.format("Submit application[%d][%s] error", app.getId(), app.getName()), e);
                app.setStatus(ApplicationStatus.FAILED.value);
                applicationMapper.updateById(app);
            }
        });
    }


    private Map<Long, Artifact> saveArtifacts(Application app, Map<Long, String> artifactIdMap) throws BizException {
        Map<Long, Artifact> result = new HashMap<>();
        if (!CollectionUtils.isEmpty(artifactIdMap)) {
            Set<Long> artifactIds = artifactIdMap.keySet();
            List<Artifact> artifacts = artifactMapper.selectList(new LambdaQueryWrapper<Artifact>().in(Artifact::getId, artifactIds));
            if (!CollectionUtils.isEmpty(artifacts)) {
                List<ApplicationArtifact> aas = new ArrayList<>();
                for (Artifact artifact : artifacts) {
                    ApplicationArtifact aa = new ApplicationArtifact();
                    aa.setApplicationId(app.getId());
                    aa.setArtifactId(artifact.getId());
                    aa.setVersionTime(app.getVersionTime());
                    aa.setDir(artifactIdMap.get(artifact.getId()));

                    aas.add(aa);
                    result.put(artifact.getId(), artifact);
                }
                applicationArtifactService.saveBatch(aas);
            }
        }
        return result;
    }


    private Application insert(CreateAppReq req) throws BizException {
        Application app = new Application();
        BeanUtils.copyProperties(req, app);
        if (req.getEnv() != null && !req.getEnv().isEmpty()) {
            try {
                app.setEnv(JsonUtil.encode(req.getEnv()));
            } catch (IOException e) {
                logger.error("Json encode error", e);
                throw new BizException(MsgCode.JSON_ENCODE_ERROR);
            }
        }
        if (req.getGroupId() == -1L) {
            app.setAbsolutePath("/");
        } else {
            Group group = groupMapper.selectById(req.getGroupId());
            if (group != null) {
                app.setAbsolutePath(group.getAbsolutePath());
            } else {
                //目录不存在则直接放置在根目录下
                app.setAbsolutePath("/");
                app.setGroupId(-1L);
            }
        }
        //重名检测
        int count = applicationMapper.selectCount(new LambdaQueryWrapper<Application>().eq(Application::getName, app.getName())
                .eq(Application::getGroupId, app.getGroupId()));
        if (count > 0) {
            throw new BizException(MsgCode.NAME_CONFLICT);
        }
        app.setStatus(ApplicationStatus.READY.value);
        app.setVersionTime(System.currentTimeMillis());
        applicationMapper.insert(app);
        logger.info("Create application {} success", app.getId());
        return app;
    }


    private void ensure(CreateAppReq req) throws BizException {
        if (StringUtils.isEmpty(req.getName())) {
            throw new BizException(MsgCode.INVALID_PARAM.code, "名称不能为空");
        }
        if (req.getCpu() == null || req.getCpu() <= 0) {
            throw new BizException(MsgCode.INVALID_PARAM.code, "Cpu数量错误");
        }
        if (req.getMemory() == null || req.getMemory() <= 0) {
            throw new BizException(MsgCode.INVALID_PARAM.code, "Memory数量错误");
        }
        if (StringUtils.isEmpty(req.getCommand())) {
            throw new BizException(MsgCode.INVALID_PARAM.code, "Command不能为空");
        }
        if (req.getGroupId() == null) {
            req.setGroupId(-1L);
        }
        if (req.getInstanceCount() == null || req.getInstanceCount() < 0) {
            req.setInstanceCount(0);
        }
    }
}
