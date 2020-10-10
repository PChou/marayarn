package com.eoi.marayarn.web.controller;

import com.eoi.marayarn.web.base.ResponseResult;
import com.eoi.marayarn.web.entity.db.Group;
import com.eoi.marayarn.web.entity.resp.GroupInfoResp;
import com.eoi.marayarn.web.exception.BizException;
import com.eoi.marayarn.web.service.GroupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@RestController
@RequestMapping("/api/group")
public class GroupController {

    @Autowired
    private GroupService groupService;

    @PostMapping
    public ResponseResult create(@RequestBody Group body) {
        try {
            groupService.create(body.getName(), body.getParentId());
            return ResponseResult.success();
        } catch (BizException e) {
            return ResponseResult.of(e.getCode(), e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    public ResponseResult delete(@PathVariable Long id) {
        try {
            groupService.delete(id);
            return ResponseResult.success();
        } catch (BizException e) {
            return ResponseResult.of(e.getCode(), e.getMessage());
        }
    }

    @GetMapping("/list/{id}")
    public ResponseResult list(@PathVariable Long id) {
        GroupInfoResp resp = groupService.listInfo(id);
        return ResponseResult.success(resp);
    }
}
