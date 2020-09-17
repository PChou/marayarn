package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.Test;

public class MaraApplicationMasterTest {
    @Test
    public void testOptions() throws Exception {
        Options options = MaraApplicationMaster.setupOptions();
        String[] args = new String[]{
                "--constraints", "node,CLUSTER",
                "--principal", "mara@AA.COM",
                "--keytab", "__kt__.keytab",
                "--cores", "2",
                "--memory", "1024",
                "--executors", "2",
                "--queue", "q"
        };
        CommandLineParser parser = new GnuParser();
        CommandLine argLine = parser.parse(options, args);
        ApplicationMasterArguments arguments = MaraApplicationMaster.toAMArguments(argLine);
        Assert.assertEquals(2, arguments.executorCores);
        Assert.assertEquals(2, arguments.numExecutors);
        Assert.assertEquals(1024, arguments.executorMemory);
        Assert.assertEquals("q", arguments.queue);
        Assert.assertEquals("node,CLUSTER", arguments.constraints);
        Assert.assertEquals("mara@AA.COM", arguments.principal);
        Assert.assertEquals("__kt__.keytab", arguments.keytab);
    }
}
