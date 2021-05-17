package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.junit.Assert;
import org.junit.Test;

public class CliTest {
    @Test
    public void commandLineParseTest_1() throws Exception {
        Options options = SubmitOptions.buildOptions();
        String[] args = new String[]{
                "--name", "marayarn",
                "--am", "file:///Users/pchou/Projects/java/marayarn/marayarn-am/target/marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar",
                "--cmd", "while true; do date; sleep 5; done",
                "--cpu", "2",
                "--memory", "1024",
                "--instance", "2",
                "--queue", "q",
                "--file", "file:///Users/pchou/Projects/java/marayarn/router.zip",
                "--file", "file:///Users/pchou/Projects/java/marayarn/router.tar.gz",
                "--file", "file:///Users/pchou/Projects/java/marayarn/logback.xml",
                "-Ea=b",
                "-Ec=d",
                "-retry=5",
        };
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        ClientArguments arguments = SubmitOptions.toClientArguments(commandLine);
        Assert.assertEquals("file:///Users/pchou/Projects/java/marayarn/marayarn-am/target/marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar", arguments.getApplicationMasterJar());
        Assert.assertEquals("marayarn", arguments.getApplicationName());
        Assert.assertEquals("while true; do date; sleep 5; done", arguments.getCommand());
        Assert.assertEquals("q", arguments.getQueue());
        Assert.assertEquals(2, arguments.getCpu());
        Assert.assertEquals(1024, arguments.getMemory());
        Assert.assertEquals(2, arguments.getInstances());
        Assert.assertEquals(2, arguments.getExecutorEnvironments().size());
        Assert.assertEquals("b", arguments.getExecutorEnvironments().get("a"));
        Assert.assertEquals("d", arguments.getExecutorEnvironments().get("c"));
        Assert.assertEquals(3, arguments.getArtifacts().size());
        Assert.assertEquals(Integer.valueOf(5), arguments.getRetryThreshold());
        for (Artifact at: arguments.getArtifacts()) {
            if (at.getLocalPath().equals("file:///Users/pchou/Projects/java/marayarn/router.zip")) {
                Assert.assertEquals(LocalResourceType.ARCHIVE, at.getType());
            } else if (at.getLocalPath().equals("file:///Users/pchou/Projects/java/marayarn/router.tar.gz")) {
                Assert.assertEquals(LocalResourceType.ARCHIVE, at.getType());
            } else if (at.getLocalPath().equals("file:///Users/pchou/Projects/java/marayarn/logback.xml")) {
                Assert.assertEquals(LocalResourceType.FILE, at.getType());
            } else {
                throw new Exception("Artifact parse error");
            }
        }
    }

    @Test
    public void testArtifactFromString() throws Exception {
        String case0 = "file:///usr/local/share/test.zip";
        String case1 = "file:///usr/local/share/test.txt";
        String case2 = "file:///usr/local/share/test.zip#dir";
        String case3 = "file:///usr/local/share/test.txt#dir";
        String case4 = "hdfs:///usr/local/share/test.zip#dir@/usr/local/hadoop2.7/conf";
        String case5 = "hdfs:///usr/local/share/test.txt@/usr/local/hadoop2.7/conf";
        Assert.assertTrue(SubmitOptions.fromString(case0).equals(new Artifact()
                .setLocalPath("file:///usr/local/share/test.zip")
                .setType(LocalResourceType.ARCHIVE)));
        Assert.assertTrue(SubmitOptions.fromString(case1).equals(new Artifact()
                .setLocalPath("file:///usr/local/share/test.txt")
                .setType(LocalResourceType.FILE)));
        Assert.assertTrue(SubmitOptions.fromString(case2).equals(new Artifact()
                .setLocalPath("file:///usr/local/share/test.zip#dir")
                .setType(LocalResourceType.ARCHIVE)));
        Assert.assertTrue(SubmitOptions.fromString(case3).equals(new Artifact()
                .setLocalPath("file:///usr/local/share/test.txt#dir")
                .setType(LocalResourceType.ARCHIVE)));
        Assert.assertTrue(SubmitOptions.fromString(case4).equals(new Artifact()
                .setLocalPath("hdfs:///usr/local/share/test.zip#dir")
                .setHadoopConfDir("/usr/local/hadoop2.7/conf")
                .setType(LocalResourceType.ARCHIVE)));
        Assert.assertTrue(SubmitOptions.fromString(case5).equals(new Artifact()
                .setLocalPath("hdfs:///usr/local/share/test.txt")
                .setHadoopConfDir("/usr/local/hadoop2.7/conf")
                .setType(LocalResourceType.FILE)));
    }
}
