package com.eoi.marayarn.logstash;

public class BulkAck {
    public Integer took;
    public Boolean ignore;
    public Boolean errors;

    public static BulkAck OK(Integer took) {
        BulkAck bulkAck = new BulkAck();
        bulkAck.took = took;
        bulkAck.ignore = false;
        bulkAck.errors = false;
        return bulkAck;
    }

    public static BulkAck FAILED(Integer took) {
        BulkAck bulkAck = new BulkAck();
        bulkAck.took = took;
        bulkAck.ignore = true;
        bulkAck.errors = true;
        return bulkAck;
    }
}
