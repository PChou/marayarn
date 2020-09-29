package com.eoi.marayarn.web.base;


import com.eoi.marayarn.web.enums.MsgCode;

public class ResponseResult {

    private String retCode;

    private String retMsg;

    private Object entity;

    private Long total;


    public ResponseResult() {
        this(MsgCode.SUCCESS);
    }

    public ResponseResult(MsgCode msgCode) {
       this(msgCode.code,msgCode.massage,null,null);
    }

    public ResponseResult(String retCode, String retMsg, Object entity, Long total) {
        this.retCode = retCode;
        this.retMsg = retMsg;
        this.entity = entity;
        this.total = total;
    }

    public static ResponseResult success(){
        return new ResponseResult(MsgCode.SUCCESS.code,MsgCode.SUCCESS.massage,null,null);
    }
    public static ResponseResult success(Object entity){
        return new ResponseResult(MsgCode.SUCCESS.code,MsgCode.SUCCESS.massage,entity,null);
    }
    public static ResponseResult success(Object entity,Long total){
        return new ResponseResult(MsgCode.SUCCESS.code,MsgCode.SUCCESS.massage,entity,total);
    }

    public static ResponseResult of(MsgCode msgCode){
        return of(msgCode,null,null);
    }
    public static ResponseResult of(MsgCode msgCode,Object entity){
        return of(msgCode,entity,null);
    }

    public static ResponseResult of(MsgCode msgCode,Object entity,Long total){
        return new ResponseResult(msgCode.code,msgCode.massage,entity,total);
    }

    public static ResponseResult of(String code,String msg){
        return of(code,msg,null,null);
    }

    public static ResponseResult of(String code,String msg,Object entity){
        return of(code,msg,entity,null);
    }
    public static ResponseResult of(String code,String msg,Object entity,Long total){
        return new ResponseResult(code,msg,entity,total);
    }

    public String getRetCode() {
        return retCode;
    }

    public ResponseResult setRetCode(String retCode) {
        this.retCode = retCode;
        return this;
    }

    public String getRetMsg() {
        return retMsg;
    }

    public ResponseResult setRetMsg(String retMsg) {
        this.retMsg = retMsg;
        return this;
    }

    public Object getEntity() {
        return entity;
    }

    public ResponseResult setEntity(Object entity) {
        this.entity = entity;
        return this;
    }

    public Long getTotal() {
        return total;
    }

    public ResponseResult setTotal(Long total) {
        this.total = total;
        return this;
    }
}
