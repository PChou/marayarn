package com.eoi.marayarn.logstash;

import com.eoi.marayarn.MaraApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Ignore;
import org.junit.Test;

public class LogstashAMPluginTest {

    @Test
    @Ignore
    public void logstashAMPluginTest_1() throws Exception {
        long ts = System.currentTimeMillis();
        MaraApplicationMaster applicationMaster = new MaraApplicationMaster();
        applicationMaster.applicationAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(ts, 1),1);
        LogstashAMPlugin logstashAMPlugin = new LogstashAMPlugin();
        applicationMaster.addPlugin(logstashAMPlugin);
        logstashAMPlugin.start(applicationMaster);
        applicationMaster.startHttpServer(20002);
        while(true) {
            Thread.sleep(5000);
        }
    }
}