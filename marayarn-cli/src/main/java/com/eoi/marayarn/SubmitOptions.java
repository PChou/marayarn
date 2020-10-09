package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.yarn.api.records.LocalResourceType;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubmitOptions extends CliOptions {
    static final Pattern fragment = Pattern.compile("#.*$");

    static boolean isArchive(String name) {
        Matcher fragmentMatcher = fragment.matcher(name);
        String clearFragment = name;
        if (fragmentMatcher.find()) {
            clearFragment = fragmentMatcher.replaceFirst("");
        }
        return clearFragment.endsWith(".tar.gz") || clearFragment.endsWith(".zip");
    }

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
                .desc("Artifacts that need upload to hdfs, support [file|hdfs|http|https|ftp]://<user>:<password>@host:port...").build();
        Option command = new com.eoi.marayarn.OptionBuilder("cmd").hasArg(true).argName("cmd").required()
                .desc("The command line").build();
        Option executorEnv = new com.eoi.marayarn.OptionBuilder("E").numberOfArgs(2).valueSeparator('=')
                .desc("Executor launch environment variable, ex: -Ea=b").build();
        Option amJars = new com.eoi.marayarn.OptionBuilder("am").hasArg(true).required()
                .desc("ApplicationMaster jar path, support [file|hdfs|http|https|ftp]://<user>:<password>@host:port...").build();
        Option principal = new com.eoi.marayarn.OptionBuilder("principal").hasArg(true)
                .desc("Principal to be used to login to KDC, while running secure HDFS").build();
        Option keytab = new com.eoi.marayarn.OptionBuilder("keytab").hasArg(true)
                .desc("The full path to the file that contains the keytab for the principal specified above").build();
        Option constraints = new com.eoi.marayarn.OptionBuilder("constraints").hasArg(true)
                .desc("The constraints string that describe the locality requirement. see document for more information").build();
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
        options.addOption(principal);
        options.addOption(keytab);
        options.addOption(constraints);
        return options;
    }

    static ClientArguments toClientArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkRequiredOption(commandLine, "cmd", "name", "am");

        ClientArguments clientArguments = new ClientArguments();
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
        clientArguments.setPrincipal(commandLine.getOptionValue("principal", null));
        clientArguments.setKeytab(commandLine.getOptionValue("keytab", null));
        clientArguments.setConstraints(commandLine.getOptionValue("constraints", null));
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

    static void printHelp() {
        System.out.printf("Action \"%s\" submit a application\n", Cli.ACTION_SUBMIT);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_SUBMIT);
        formatter.printHelp(syntax, buildOptions());
    }
}
