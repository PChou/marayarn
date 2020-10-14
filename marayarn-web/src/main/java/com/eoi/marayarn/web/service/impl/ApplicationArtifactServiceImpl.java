package com.eoi.marayarn.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.eoi.marayarn.web.entity.db.ApplicationArtifact;
import com.eoi.marayarn.web.mapper.ApplicationArtifactMapper;
import com.eoi.marayarn.web.service.ApplicationArtifactService;
import org.springframework.stereotype.Service;

/**
 * Created by wenbo.gong on 2020/10/13
 */
@Service
public class ApplicationArtifactServiceImpl extends ServiceImpl<ApplicationArtifactMapper, ApplicationArtifact> implements ApplicationArtifactService {
}
