package com.eoi.marayarn.web.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.eoi.marayarn.web.entity.db.Application;
import com.eoi.marayarn.web.entity.db.Group;
import com.eoi.marayarn.web.entity.resp.GroupInfoResp;
import com.eoi.marayarn.web.enums.ApplicationStatus;
import com.eoi.marayarn.web.enums.MsgCode;
import com.eoi.marayarn.web.exception.BizException;
import com.eoi.marayarn.web.mapper.ApplicationMapper;
import com.eoi.marayarn.web.mapper.GroupMapper;
import com.eoi.marayarn.web.service.GroupService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@Service
public class GroupServiceImpl implements GroupService {

    @Autowired
    private GroupMapper groupMapper;

    @Autowired
    private ApplicationMapper applicationMapper;

    @Override
    public void create(String name, Long parentId) throws BizException {
        if (StringUtils.isEmpty(name)){
            throw new BizException(MsgCode.INVALID_PARAM);
        }
        if (parentId == null) {
            parentId = -1L;
        }
        //重名检测
        int count = groupMapper.selectCount(new LambdaQueryWrapper<Group>().eq(Group::getName, name).eq(Group::getParentId, parentId));
        if (count > 0) {
            throw new BizException(MsgCode.NAME_CONFLICT);
        }

        Group group = new Group();
        group.setName(name);

        //确保父级目录存在
        if (parentId != -1) {
            Group parent = groupMapper.selectById(parentId);
            if (parent == null) {
                throw new BizException(MsgCode.PARENT_NOT_EXIST);
            }
            group.setAbsolutePath(parent.getAbsolutePath() + name + "/");
        } else {
            group.setAbsolutePath("/" + name + "/");
        }
        group.setParentId(parentId);
        groupMapper.insert(group);
    }

    @Override
    public void delete(Long id) throws BizException {
        if (id != null && id != -1) {
            groupMapper.deleteById(id);
            //TODO 删除该目录下的应用, 递归
        }
    }

    @Override
    public GroupInfoResp listInfo(Long id) {

        //获取当前目录下的子目录
        List<Group> children = groupMapper.selectList(new LambdaQueryWrapper<Group>().eq(Group::getParentId, id));

        List<GroupInfoResp.Item> groupInfos = new ArrayList<>();
        for (Group child : children) {
            int cpu = 0;
            int memory = 0;
            int running = 0;
            int total = 0;

            List<Application> apps = applicationMapper.selectList(new LambdaQueryWrapper<Application>().likeRight(Application::getAbsolutePath, child.getAbsolutePath()));
            for (Application app : apps) {
                cpu += app.getCpu();
                memory += app.getMemory();
                total += app.getInstanceCount();
                if (ApplicationStatus.RUNNING.value.equalsIgnoreCase(app.getStatus())) {
                    running += app.getInstanceCount();
                }
            }
            groupInfos.add(GroupInfoResp.Item.builder().id(child.getId()).name(child.getName())
                    .cpu(cpu).memory(memory).runningInstance(running).totalInstance(total).build());
        }

        //获取当前目录下的应用
        List<Application> applications = applicationMapper.selectList(new LambdaQueryWrapper<Application>().eq(Application::getGroupId, id));

        List<GroupInfoResp.Item> appInfos = new ArrayList<>();
        for (Application app : applications) {
            GroupInfoResp.Item appItem = GroupInfoResp.Item.builder().id(app.getId()).name(app.getName()).cpu(app.getCpu())
                    .memory(app.getMemory()).status(app.getStatus()).totalInstance(app.getInstanceCount())
                    .runningInstance(ApplicationStatus.RUNNING.value.equalsIgnoreCase(app.getStatus()) ? app.getInstanceCount() : 0).build();
            appInfos.add(appItem);
        }

        GroupInfoResp resp = new GroupInfoResp();
        resp.setGroupInfos(groupInfos);
        resp.setAppInfos(appInfos);

        return resp;
    }

}
