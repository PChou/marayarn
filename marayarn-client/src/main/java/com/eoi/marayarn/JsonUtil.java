package com.eoi.marayarn;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

public class JsonUtil {
    public static final ObjectMapper _mapper = new ObjectMapper();

    static {
        _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        SimpleModule s = new SimpleModule();
        _mapper.registerModule(s);
    }

    public static String encode(Object value) throws IOException {
        if(value == null) {
            return null;
        }
        return _mapper.writeValueAsString(value);
    }

    public static String print(Object value) throws IOException {
        if(value == null) {
            return null;
        }
        return _mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value);
    }

    public static <T> T decode(String value, Class<T> valueType) throws IOException {
        if(value == null) {
            return null;
        }
        return _mapper.readValue(value, valueType);
    }

    public static <T> T decode(String value, TypeReference<T> valueTypeRef) throws IOException {
        if(value == null) {
            return null;
        }
        return _mapper.readValue(value, valueTypeRef);
    }
}
