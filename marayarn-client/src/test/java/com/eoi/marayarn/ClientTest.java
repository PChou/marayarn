package com.eoi.marayarn;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Ignore("need hadoop yarn environment to launch the test case")
public class ClientTest {

    public final static String AM_JAR_NAME = "marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar";

    private String getProjectRoot() {
        // 无论是mvn命令还是intellij，在运行测试的时候都会设置PWD为当前module目录
        File modulePath = new File(System.getenv("PWD"));
        return modulePath.getParent();
    }

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void Client_initConfigurationTest_1() {
        Client client = new Client();
        ClientArguments arguments = new ClientArguments();
        arguments.setHadoopConfDir(getProjectRoot() + "/hadoop");
        client.initConfiguration(arguments);
        String fs = client.yarnConfiguration.get("fs.defaultFS");
        Assert.assertEquals("hdfs://eoiNameService", fs);
    }

    @Test
    public void Client_initConfigurationTest_2() {
        Client client = new Client();
        ClientArguments arguments = new ClientArguments();
        environmentVariables.set("HADOOP_CONF_DIR", getProjectRoot() + "/hadoop");
        client.initConfiguration(arguments);
        String fs = client.yarnConfiguration.get("fs.defaultFS");
        Assert.assertEquals("hdfs://eoiNameService", fs);
    }

    @Test
    public void clientTest_simple_shell_command() throws Exception {
        // environmentVariables.set("HADOOP_PROXY_USER", "root");
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file://" + getProjectRoot() + "/marayarn-am/target/" + AM_JAR_NAME);
        arguments.setApplicationName("clientTest_simple_shell_command");
        arguments.setHadoopConfDir(getProjectRoot() + "/hadoop-test");
        arguments.setCommand("while true; do date; sleep 5; done");
        arguments.setInstances(1);
        // arguments.setUser("test1");
        Client client = new Client();
        ApplicationReport report = client.launch(arguments);
        System.out.println(report.getTrackingUrl());
    }

    @Test
    public void clientTest_simple_shell_command_with_env() throws Exception {
        // environmentVariables.set("HADOOP_PROXY_USER", "root");
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file://" + getProjectRoot() + "/marayarn-am/target/" + AM_JAR_NAME);
        arguments.setApplicationName("clientTest_simple_shell_command_with_env");
        arguments.setHadoopConfDir(getProjectRoot() + "/hadoop-test");
        arguments.setCommand("while true; do date; sleep 5; echo $env; done");
        arguments.setInstances(1);
        arguments.setExecutorEnvironments(new HashMap(){{
            put("env", "hello");
        }});
        // arguments.setUser("test1");
        Client client = new Client();
        ApplicationReport report = client.launch(arguments);
        System.out.println(report.getTrackingUrl());
    }

    @Test
    public void clientTest_artifact_cross_hadoop() throws Exception {
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file://" + getProjectRoot() + "/marayarn-am/target/" + AM_JAR_NAME);
        arguments.setApplicationName("clientTest_artifact_cross_hadoop");
        arguments.setHadoopConfDir(getProjectRoot() + "/hadoop");
        arguments.setCommand("while true; do date; sleep 5; echo $env; done");
        arguments.setInstances(1);
        List<Artifact> artifacts = new ArrayList<>();
        Artifact routerTar = new Artifact()
                .setLocalPath("hdfs://cdhnode2:8020/user/root/marayarn/upload/20201224/logstash.zip")
                .setHadoopConfDir(getProjectRoot() + "/hadoop-test")
                .setType(LocalResourceType.ARCHIVE);
        artifacts.add(routerTar);
        arguments.setArtifacts(artifacts);
        Client client = new Client();
        ApplicationReport report = client.launch(arguments);
        System.out.println(report.getTrackingUrl());
    }

    @Test
    public void clientTest3() throws Exception {
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file:///Users/pchou/Projects/java/marayarn/marayarn-am/target/marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar");
        arguments.setApplicationName("marayarn_test2");
        List<Artifact> artifacts = new ArrayList<>();
        Artifact routerTar = new Artifact()
                // .setLocalPath("file:///Users/pchou/Downloads/logstash-7.3.0-eoi.tar.gz#dir")
                // .setLocalPath("hdfs://eoiNameService/user/pchou/logstash-7.3.0-eoi.tar.gz")
                .setLocalPath("http://192.168.31.55:9999/jaxjobjarlib/spark/logstash-7.3.0-eoi.tar.gz#dir")
                // http://commons.apache.org/proper/commons-net/apidocs/org/apache/commons/net/ftp/FTPClient.html#ACTIVE_LOCAL_DATA_CONNECTION_MODE
                // .setLocalPath("ftp://anonymous:anonymous@192.168.31.84/0.Sharplook_Release/LogAnalysis_Center/3.7/mave/logstash-7.3.0-eoi.tar.gz")
                // .setLocalPath("ftp://.%2Fftp:Password01!@192.168.31.84/0.Sharplook_Release/LogAnalysis_Center/3.7/mave/logstash-7.3.0-eoi.tar.gz")
                // hadoop 2.10+ support sftp filesystem https://stackoverflow.com/questions/13816441/sftp-file-system-in-hadoop
                // but test not work
                // .setLocalPath("sftp://root:Eoi123456!@192.168.31.55/home/jax-dist/jax-yarn/jax/jar_lib/spark/logstash-7.3.0-eoi.tar.gz")
                .setType(LocalResourceType.ARCHIVE);
        artifacts.add(routerTar);
        arguments.setArtifacts(artifacts);
        arguments.setCommand("dir/logstash/bin/logstash -e 'input { stdin { } } output { stdout {} }'");
        Client client = new Client();
        ApplicationReport report = client.launch(arguments);
        System.out.println(report.getTrackingUrl());
    }
}
