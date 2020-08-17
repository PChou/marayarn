package com.eoi.marayarn;

import com.eoi.marayarn.http.*;
import com.eoi.marayarn.http.model.AckResponse;
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
    private List<Handler> handlers;

    public HttpRequestHandler(MaraApplicationMaster applicationMaster) {
        this.handlers = new ArrayList<>();
        this.handlers.add(new ApplicationHandler(applicationMaster));
        // PageHandler should be always the last handler added
        this.handlers.add(new PageHandler());
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
            if (params != null) {
                urlParams = params;
                handler = h;
                break;
            }
        }
        FullHttpResponse response;
        try {
            if(handler != null) {
                int len = req.content().readableBytes();
                byte[] buffer = new byte[len];
                req.content().readBytes(buffer);
                ProcessResult result = handler.process(urlParams, req.method(), buffer);
                if (result != null) {
                    response = new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK,
                            Unpooled.copiedBuffer(result.body));
                } else {
                    response = new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK);
                }
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, result.contentType);
            } else {
                throw new HandlerErrorException(HttpResponseStatus.NOT_FOUND, "no match handler");
            }
        } catch (HandlerErrorException ex) {
            response = createFromException(ex);
        }
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private FullHttpResponse createFromException(HandlerErrorException exception)
            throws JsonProcessingException {
        AckResponse response = AckResponse.build(exception.status.codeAsText().toString(), exception.message);
        byte[] buffer = JsonUtil._mapper.writeValueAsBytes(response);
        FullHttpResponse httpResp = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                exception.status,
                Unpooled.copiedBuffer(buffer));
        httpResp.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        return httpResp;
    }
}
