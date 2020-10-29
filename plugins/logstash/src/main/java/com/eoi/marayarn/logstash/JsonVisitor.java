package com.eoi.marayarn.logstash;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.InputStream;
import java.util.Map;

public abstract class JsonVisitor {
    public static final ObjectMapper _mapper = new ObjectMapper();
    static {
        _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        SimpleModule s = new SimpleModule();
        _mapper.registerModule(s);
    }

    private Map<String, Object> parsedMap;

    public JsonVisitor(String json)  {
        try {
            parsedMap = _mapper.readValue(json, new TypeReference<Map<String, Object>>() { });
        } catch (Exception ex) { }
    }

    public JsonVisitor(InputStream stream) {
        try {
            parsedMap = _mapper.readValue(stream, new TypeReference<Map<String, Object>>() { });
        } catch (Exception ex) { }
    }

    public JsonVisitor(Map<String, Object> parsedMap) {
        this.parsedMap = parsedMap;
    }

    public void visit() {
        visit(parsedMap, "");
    }

    protected void visit(Map<String, Object> root, String path) {
        if (root == null) {
            return;
        }
        for (Map.Entry<String, Object> item: root.entrySet()) {
            String itemPath = path + "." + item.getKey();
            boolean go = internalVisit(itemPath, item.getValue());
            if (!go) {
                continue;
            }
            if (item.getValue() instanceof Map) {
                visit((Map<String, Object>)item.getValue(), itemPath);
            }
        }
    }

    /**
     * visit value and path, return false if need break (stop to visit the sub tree)
     * @param path the key path like '.process.cpu.1m' (always start with .)
     * @param value the object of the path
     * @return return false if need break (stop to visit the sub tree), return true if need continue
     */
    protected abstract boolean internalVisit(String path, Object value);
}
