package com.eoi.marayarn;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

@Ignore("need hadoop yarn environment with kerberos enabled to launch the test case")
public class KerberosTest {

    public final static String AM_JAR_NAME = "marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar";

    private String getProjectRoot() {
        // 无论是mvn命令还是intellij，在运行测试的时候都会设置PWD为当前module目录
        File modulePath = new File(System.getenv("PWD"));
        return modulePath.getParent();
    }

    @Test
    public void test_no_constraints() throws Exception {
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file://" + getProjectRoot() + "/marayarn-am/target/" + AM_JAR_NAME);
        arguments.setApplicationName("test_no_constraints");
        arguments.setHadoopConfDir(getProjectRoot() + "/hadoop-kerberos");
        arguments.setPrincipal("mara@ALANWANG.COM");
        arguments.setKeytab("file://" + getProjectRoot() + "/hadoop-kerberos/mara.keytab");
        arguments.setCommand("while true; do date; sleep 5; done");
        arguments.setInstances(4);
        Client client = new Client();
        ApplicationReport report = client.launch(arguments);
        System.out.println(report.getTrackingUrl());

        // 1. 验证初次实例是否正确
        // 2. 验证扩容后是否正确
        // 3. 验证变更(实例数和资源同时变更)后是否正确
        // 4. 验证变更增加Constraints后是否正确
    }


    @Test
    public void test_with_constraints() throws Exception {
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file://" + getProjectRoot() + "/marayarn-am/target/" + AM_JAR_NAME);
        arguments.setApplicationName("test_no_constraints");
        arguments.setHadoopConfDir(getProjectRoot() + "/hadoop-kerberos");
        arguments.setPrincipal("mara@ALANWANG.COM");
        arguments.setKeytab("file://" + getProjectRoot() + "/hadoop-kerberos/mara.keytab");
        arguments.setCommand("while true; do date; sleep 5; done");
        arguments.setInstances(2);
        arguments.setConstraints("node,CLUSTER");
        Client client = new Client();
        ApplicationReport report = client.launch(arguments);
        System.out.println(report.getTrackingUrl());

        // 1. 验证初次实例是否正确
        // 2. 验证改为`node,CLUSTER,vm3198`, 并扩容到4
        // 3. 验证改为`node,UNIQUE`, 并扩容到3
        // 4. 验证改为`node,GROUP_BY`, 并扩容到10，由于vm3195可能分配不出第4个，所以应该等1min钟后，飘到vm3196:
        /**
         * 20/09/15 14:49:46 WARN marayarn.YarnAllocator: Pending delay is over 60s, cancel 1 requests at ContainerLocation{node=[vm3195], rack=null, (2147483647/1/3)}
         * 20/09/15 14:49:51 INFO marayarn.YarnAllocator: Requesting 1 executor containers at ContainerLocation{node=[vm3196], rack=null, (2147483647/0/3)}, each with resource <memory:512, vCores:1>
         */
    }

    // /rack01: vm3195/vm3198
    // /rack02: vm3196
    @Test
    public void test_with_constraints_rack() throws Exception {
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file://" + getProjectRoot() + "/marayarn-am/target/" + AM_JAR_NAME);
        arguments.setApplicationName("test_no_constraints");
        arguments.setHadoopConfDir(getProjectRoot() + "/hadoop-kerberos");
        arguments.setPrincipal("mara@ALANWANG.COM");
        arguments.setKeytab("file://" + getProjectRoot() + "/hadoop-kerberos/mara.keytab");
        arguments.setCommand("while true; do date; sleep 5; done");
        arguments.setInstances(6);
        arguments.setConstraints("rack,CLUSTER,/rack01");
        Client client = new Client();
        ApplicationReport report = client.launch(arguments);
        System.out.println(report.getTrackingUrl());

        // 1. 验证首次结果
        // 2. 改为rack,GROUP_BY，扩容到4
    }
}
