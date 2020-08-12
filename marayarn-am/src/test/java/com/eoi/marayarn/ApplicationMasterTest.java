package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.Test;

public class ApplicationMasterTest {
    @Test
    public void testOptions() throws Exception {
        Options options = ApplicationMaster.setupOptions();
        String[] args = new String[]{
                "--cmd", "while true; do date; sleep 5; done",
                "--cores", "2",
                "--memory", "1024",
                "--executors", "2",
                "--queue", "q"
        };
        CommandLineParser parser = new GnuParser();
        CommandLine argLine = parser.parse(options, args);
        ApplicationMasterArguments arguments = ApplicationMaster.toAMArguments(argLine);
        Assert.assertEquals(2, arguments.executorCores);
        Assert.assertEquals(2, arguments.numExecutors);
        Assert.assertEquals(1024, arguments.executorMemory);
        Assert.assertEquals("q", arguments.queue);
        Assert.assertEquals("while true; do date; sleep 5; done", arguments.commandLine);
    }
}
