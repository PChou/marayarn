package com.eoi.marayarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.function.Predicate;

public class IntergrationTest {

    public final static String AM_JAR_NAME = "marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar";

    protected static MiniYarnCluster yarnCluster;

    private String getProjectRoot() {
        // 无论是mvn命令还是intellij，在运行测试的时候都会设置PWD为当前module目录
        File modulePath = new File(System.getenv("PWD"));
        return modulePath.getParent();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        yarnCluster = new MiniYarnCluster("IntergrationTest", 2);
        yarnCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        yarnCluster.close();
    }

    public static <T> void waitForAction(int timeoutSec, T target, Predicate<T> predicate) {
        int iterWait = timeoutSec * 100;
        do {
            try {
                Thread.sleep(iterWait);
                if (predicate.test(target)) {
                    return;
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            iterWait += 1;
        } while (iterWait * 1000 >= timeoutSec);
        throw new RuntimeException("timeout");
    }

    private ApplicationInfo fetchApplicationInfo(String originalTrackingUrl) throws Exception {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            HttpGet httpGet = new HttpGet(originalTrackingUrl + "/api/app");
            CloseableHttpResponse response = client.execute(httpGet);
            return JsonUtil._mapper.readValue(response.getEntity().getContent(), ApplicationInfo.class);
        }
    }

    @Test
    public void basicShellTest() throws Exception {
        Configuration configuration = yarnCluster.getConfiguration();
        ClientArguments arguments = new ClientArguments();
        arguments.setConfiguration((YarnConfiguration) configuration);
        arguments.setApplicationMasterJar("file://" + getProjectRoot() + "/marayarn-am/target/" + AM_JAR_NAME);
        arguments.setApplicationName("basicShellTest");
        arguments.setCommand("while true; do date; sleep 5; done");
        arguments.setInstances(1);
        arguments.setCpu(1);
        arguments.setMemory(128);
        arguments.setaMEnvironments(new HashMap(){{
            put("JAVA_HOME", System.getProperty("java.home"));
            put("HADOOP_CONF_DIR", yarnCluster.getHadoopConfDir());
        }});
        Client client = new Client();
        ApplicationReport report = client.launch(arguments);
        waitForAction(30, arguments, a -> {
            Client c = new Client();
            a.setApplicationId(report.getApplicationId().toString());
            try {
                ApplicationReport applicationReport = c.get(a);
                return applicationReport.getYarnApplicationState() == YarnApplicationState.RUNNING;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        Client c = new Client();
        arguments.setApplicationId(report.getApplicationId().toString());
        ApplicationReport applicationReport = c.get(arguments);
        String originalTrackingUrl = applicationReport.getOriginalTrackingUrl();
        waitForAction(30, originalTrackingUrl, url -> {
            try {
                ApplicationInfo applicationInfo = fetchApplicationInfo(url);
                if (applicationInfo.getNumRunningExecutors() == 1) {
                    return true;
                }
            } catch (Exception ex) {}
            return false;
        });
        ApplicationInfo info = fetchApplicationInfo(originalTrackingUrl);
        Assert.assertEquals(1, info.getContainers().size());
        Assert.assertEquals(0, info.getCompletedContainers().size());
        Assert.assertEquals(1, info.getContainers().get(0).getVcore());
        Assert.assertEquals(128, info.getContainers().get(0).getMemory());
    }
}
