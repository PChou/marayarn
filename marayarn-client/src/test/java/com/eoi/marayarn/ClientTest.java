package com.eoi.marayarn;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ClientTest {
    @Test
    public void clientTest1() throws Exception {
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file:///Users/pchou/Projects/java/marayarn/marayarn-am/target/marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar");
        arguments.setApplicationName("marayarn_test");
        arguments.setCommand("while true; do date; sleep 5; done");
        Client client = new Client(arguments);
        ApplicationReport report = client.launch();
        System.out.println(report.getTrackingUrl());
    }

    @Test
    public void clientTest2() throws Exception {
        ClientArguments arguments = new ClientArguments();
        arguments.setApplicationMasterJar("file:///Users/pchou/Projects/java/marayarn/marayarn-am/target/marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar");
        arguments.setApplicationName("marayarn_test2");
        List<Artifact> artifacts = new ArrayList<>();
        Artifact routerTar = new Artifact()
                .setLocalPath("file:///Users/pchou/Projects/java/marayarn/router-2.1.0.tar.gz")
                .setType(LocalResourceType.ARCHIVE);
        Artifact applicationYaml = new Artifact()
                .setLocalPath("file:///Users/pchou/Projects/java/marayarn/application.yml")
                .setType(LocalResourceType.FILE);
        Artifact logback = new Artifact()
                .setLocalPath("file:///Users/pchou/Projects/java/marayarn/logback.xml")
                .setType(LocalResourceType.FILE);
        artifacts.add(routerTar);
        artifacts.add(applicationYaml);
        artifacts.add(logback);
        arguments.setArtifacts(artifacts);
        arguments.setCommand("{{JAVA_HOME}}/bin/java -jar -Dlogback.configurationFile=logback.xml router-2.1.0.tar.gz/router/router-*.jar application.yml");
        Client client = new Client(arguments);
        ApplicationReport report = client.launch();
        System.out.println(report.getTrackingUrl());
    }
}
