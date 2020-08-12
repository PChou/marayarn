package com.eoi.marayarn.http;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class JsonUtil {
    public static final ObjectMapper _mapper = new ObjectMapper();

    static {
        _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        SimpleModule s = new SimpleModule();
        _mapper.registerModule(s);
    }
}
