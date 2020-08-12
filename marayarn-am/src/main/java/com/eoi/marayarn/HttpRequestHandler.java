package com.eoi.marayarn;

import com.eoi.marayarn.http.ApplicationHandler;
import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.JsonUtil;
import com.eoi.marayarn.http.model.ErrorResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private ApplicationMaster applicationMaster;
    private List<Handler> handlers;

    public HttpRequestHandler(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.handlers = new ArrayList<>();
        this.handlers.add(new ApplicationHandler(applicationMaster));
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        Handler handler = null;
        Map<String, String> urlParams = null;
        for (Handler h: this.handlers) {
            Map<String, String> params = h.match(req.uri(), req.method());
            if (params == null) {
                continue;
            }
            if (urlParams == null || urlParams.size() < params.size()) {
                urlParams = params;
                handler = h;
            }
        }
        FullHttpResponse response;
        try {
            if(handler != null) {
                int len = req.content().readableBytes();
                byte[] buffer = new byte[len];
                req.content().readBytes(buffer);
                byte[] result = handler.process(urlParams, req.method(), buffer);
                if (result != null) {
                    response = new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK,
                            Unpooled.copiedBuffer(result));
                } else {
                    response = new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK);
                }
            } else {
                throw new Handler.HandlerErrorException(HttpResponseStatus.NOT_FOUND, "no match handler");
            }
        } catch (Handler.HandlerErrorException ex) {
            response = createFromException(ex);
        }
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private FullHttpResponse createFromException(Handler.HandlerErrorException exception)
            throws JsonProcessingException {
        ErrorResponse response = new ErrorResponse();
        response.errMessage = exception.message;
        byte[] buffer = JsonUtil._mapper.writeValueAsBytes(response);
        return new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                exception.status,
                Unpooled.copiedBuffer(buffer));
    }
}
