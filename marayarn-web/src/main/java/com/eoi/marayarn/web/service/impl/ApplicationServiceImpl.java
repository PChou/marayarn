package com.eoi.marayarn.web.service.impl;

import com.eoi.marayarn.web.mapper.ApplicationMapper;
import com.eoi.marayarn.web.service.ApplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by wenbo.gong on 2020/9/29
 */
@Service
public class ApplicationServiceImpl implements ApplicationService {

    @Autowired
    private ApplicationMapper applicationMapper;

}
