package com.eoi.marayarn.web.exception;

import com.eoi.marayarn.web.base.ResponseResult;
import com.eoi.marayarn.web.enums.MsgCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;

@ResponseBody
@RestControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseResult exceptionHandler(HttpServletRequest request, Exception ex) {
        logger.error("内部异常", ex);
        if (ex instanceof BizException) {
            return ResponseResult.of(((BizException) ex).getCode(), ex.getMessage());
        }
        return ResponseResult.of(MsgCode.SYSTEM_ERROR);
    }
}
