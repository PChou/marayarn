package com.eoi.marayarn.http;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;

public interface Handler {
    /**
     * 匹配uri和method，返回匹配出来的key/value对。key/value越多表示匹配度越大
     * 实现的时候，一般采用正则匹配，返回匹配的分组数量。分组数量越多一般表示匹配越精准
     * @param uri 请求的uri(path部分)
     * @param method http method
     * @return 匹配出来的key/value对，如果为null表示不匹配
     */
    Map<String, String> match(String uri, HttpMethod method);

    /**
     * 处理请求, 如果处理成功返回body内容，如果处理失败抛出异常
     * @param urlParams url的参数，通过match匹配得到的
     * @param body body，可能为空
     * @return 返回的body内容
     * @throws HandlerErrorException
     */
    byte[] process(Map<String, String> urlParams, HttpMethod method, byte[] body) throws HandlerErrorException;

    class HandlerErrorException extends Exception {
        public HttpResponseStatus status;
        public String message;
        public HandlerErrorException(HttpResponseStatus status, String message) {
            this.status = status;
            this.message = message;
        }
    }
}
