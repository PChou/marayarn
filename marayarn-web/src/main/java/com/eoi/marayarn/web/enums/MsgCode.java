package com.eoi.marayarn.web.enums;

/**
 * Created by wenbo.gong on 2020/6/1
 */
public enum MsgCode {

    SYSTEM_ERROR("9999", "系统错误"),
    SUCCESS("0000", "成功"),
    INVALID_PARAM("1001", "非法参数"),
    NOT_EXIST("1002", "记录不存在"),

    NAME_CONFLICT("1000001","名称重复"),
    PARENT_NOT_EXIST("1000002","父级目录不存在")
    ;

    public final String code;
    public final String massage;

    MsgCode(String code, String massage) {
        this.code = code;
        this.massage = massage;
    }


}
