package com.eoi.marayarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Cli {
    static final Logger logger = LoggerFactory.getLogger(Cli.class);

    static Options buildOptions() {
        Options options = new Options();
        Option help = new com.eoi.marayarn.OptionBuilder("help").hasArg(false)
                .desc("Print help").build();
        Option name = new com.eoi.marayarn.OptionBuilder("name").hasArg(true).argName("name").required()
                .desc("Set name of the Application").build();
        Option queue = new com.eoi.marayarn.OptionBuilder("queue").hasArg(true).argName("name")
                .desc("The queue name of resource which tasks run in").build();
        Option cpu = new com.eoi.marayarn.OptionBuilder("cpu").hasArg(true).argName("int")
                .desc("The vcpu core count of each task (not include am)").build();
        Option memory = new com.eoi.marayarn.OptionBuilder("memory").hasArg(true).argName("int")
                .desc("The memory in MB of each task (not include am)").build();
        Option instance = new com.eoi.marayarn.OptionBuilder("instance").hasArg(true).argName("int")
                .desc("The number of instance of the application (not include am)").build();
        Option files = new com.eoi.marayarn.OptionBuilder("file").hasArgs().argName("file://<LocalPath>")
                .desc("Artifacts that need upload to hdfs").build();
        Option command = new com.eoi.marayarn.OptionBuilder("cmd").hasArg(true).argName("cmd").required()
                .desc("The command line").build();
        Option executorEnv = new com.eoi.marayarn.OptionBuilder("E").numberOfArgs(2).valueSeparator('=')
                .desc("Executor launch environment variable, ex: -Ea=b").build();
        Option amJars = new com.eoi.marayarn.OptionBuilder("am").hasArg(true).required()
                .desc("ApplicationMaster jar path").build();
        options.addOption(help);
        options.addOption(name);
        options.addOption(queue);
        options.addOption(cpu);
        options.addOption(memory);
        options.addOption(instance);
        options.addOption(files);
        options.addOption(command);
        options.addOption(executorEnv);
        options.addOption(amJars);
        return options;
    }

    static boolean isArchive(String name) {
        return name.endsWith(".tar.gz") || name.endsWith(".zip");
    }

    static ClientArguments toClientArguments(CommandLine commandLine) throws InvalidCommandLineException {
        ClientArguments clientArguments = new ClientArguments();
        if (!commandLine.hasOption("cmd")
            || !commandLine.hasOption("name")
            || !commandLine.hasOption("am")) {
            throw new InvalidCommandLineException();
        }
        String cmd = commandLine.getOptionValue("cmd");
        String name = commandLine.getOptionValue("name");
        String amJar = commandLine.getOptionValue("am");
        clientArguments.setCommand(cmd);
        clientArguments.setApplicationName(name);
        clientArguments.setApplicationMasterJar(amJar);
        clientArguments.setQueue(commandLine.getOptionValue("queue"));
        clientArguments.setCpu(Integer.parseInt(commandLine.getOptionValue("cpu", "1")));
        clientArguments.setMemory(Integer.parseInt(commandLine.getOptionValue("memory", "512")));
        clientArguments.setInstances(Integer.parseInt(commandLine.getOptionValue("instance", "1")));
        Properties envsProps = commandLine.getOptionProperties("E");
        Map<String, String> executorEnvs = new HashMap<>();
        if (envsProps != null) {
            for (Map.Entry<Object, Object> item: envsProps.entrySet()) {
                executorEnvs.put(item.getKey().toString(), item.getValue().toString());
            }
        }
        clientArguments.setExecutorEnvironments(executorEnvs);
        String[] files = commandLine.getOptionValues("file");
        if (files != null) {
            List<Artifact> artifacts = new ArrayList<>();
            for (String file : files) {
                Artifact artifact;
                if (isArchive(file)) {
                    artifact = new Artifact().setLocalPath(file).setType(LocalResourceType.ARCHIVE);
                } else {
                    artifact = new Artifact().setLocalPath(file).setType(LocalResourceType.FILE);
                }
                artifacts.add(artifact);
            }
            clientArguments.setArtifacts(artifacts);
        }
        return clientArguments;
    }

    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLineParser parser = new GnuParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "com.eoi.marayarn.Cli", options );
                System.exit(0);
            }
            ClientArguments clientArguments = toClientArguments(commandLine);
            Client client = new Client(clientArguments);
            ApplicationReport report = client.launch();
            logger.info("Tracking url: {}", report.getTrackingUrl());
        } catch (Exception ex) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "com.eoi.marayarn.Cli", options );
        }
    }

    static class InvalidCommandLineException extends Exception {}
}
