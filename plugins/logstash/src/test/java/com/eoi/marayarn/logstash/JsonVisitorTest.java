package com.eoi.marayarn.logstash;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonVisitorTest {

    static class FlattenJsonVisitor extends JsonVisitor {

        private LinkedHashMap<String, Object> flattenEntries = new LinkedHashMap<>();

        public FlattenJsonVisitor(InputStream json) {
            super(json);
        }

        @Override
        protected boolean internalVisit(String path, Object value) {
            if (!(value instanceof Map)) {
                flattenEntries.put(path, value);
            }
            return true;
        }

        public void print() {
            for (Map.Entry<String, Object> entry: flattenEntries.entrySet()) {
                if (entry.getValue() != null) {
                    System.out.printf("%s=%s\n", entry.getKey(), entry.getValue().toString());
                } else {
                    System.out.printf("%s=null\n", entry.getKey());
                }
            }
        }
    }

    @Test
    public void JsonVisitorTest_1() {
        FlattenJsonVisitor flattenJsonVisitor =
                new FlattenJsonVisitor(getClass().getResourceAsStream("/test.json"));
        flattenJsonVisitor.visit();
        Assert.assertEquals(1, flattenJsonVisitor.flattenEntries.get(".process.cpu.percent"));
        Assert.assertEquals(106, flattenJsonVisitor.flattenEntries.get(".process.open_file_descriptors"));
        Assert.assertEquals("45e35ccc-962c-4613-9e9e-dad635085a9d", flattenJsonVisitor.flattenEntries.get(".logstash.uuid"));
        Assert.assertEquals(341, flattenJsonVisitor.flattenEntries.get(".jvm.gc.collectors.old.collection_time_in_millis"));
        flattenJsonVisitor.print();
    }
}