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

    static Options buildOptions() {
        Options options = new Options();
        Option name = new OptionBuilder("name").hasArg(true).argName("name").required()
                .desc("Set name of the Application").build();
        Option queue = new OptionBuilder("queue").hasArg(true).argName("name")
                .desc("The queue name of resource which tasks run in").build();
        Option cpu = new OptionBuilder("cpu").hasArg(true).argName("int")
                .desc("The vcpu core count of each task (not include am)").build();
        Option memory = new OptionBuilder("memory").hasArg(true).argName("int")
                .desc("The memory in MB of each task (not include am)").build();
        Option ammemory = new OptionBuilder("ammemory").hasArg(true).argName("int")
                .desc("The memory in MB of application master").build();
        Option instance = new OptionBuilder("instance").hasArg(true).argName("int")
                .desc("The number of instance of the application (not include am)").build();
        Option files = new OptionBuilder("file").hasArgs().argName("file://<LocalPath>")
                .desc("Artifacts that need upload to hdfs, support [file|hdfs|http|https|ftp]://<user>:<password>@host:port...").build();
        Option command = new OptionBuilder("cmd").hasArg(true).argName("cmd").required()
                .desc("The command line").build();
        Option amEnv = new OptionBuilder("e").numberOfArgs(2).valueSeparator('=')
                .desc("ApplicationMaster launch environment variable, ex: -e 'a=b'").build();
        Option executorEnv = new OptionBuilder("E").numberOfArgs(2).valueSeparator('=')
                .desc("Executor launch environment variable, ex: -E 'a=b'").build();
        Option amJars = new OptionBuilder("am").hasArg(true).required()
                .desc("ApplicationMaster jar path, support [file|hdfs|http|https|ftp]://<user>:<password>@host:port...").build();
        Option principal = new OptionBuilder("principal").hasArg(true)
                .desc("Principal to be used to login to KDC, while running secure HDFS").build();
        Option keytab = new OptionBuilder("keytab").hasArg(true)
                .desc("The full path to the file that contains the keytab for the principal specified above").build();
        Option constraints = new OptionBuilder("constraints").hasArg(true)
                .desc("The constraints string that describe the locality requirement. see document for more information").build();
        Option proxyUser = new OptionBuilder("user").hasArg(true)
                .desc("User to impersonate when submitting the application. This argument does not work with --principal / --keytab").build();
        Option retry = new OptionBuilder("retry").hasArg(true)
                .desc("Retry threshold for failed containers").build();
        Option javaOptions = new OptionBuilder("opt").hasArgs()
                .desc("Pass java options to ApplicationMaster").build();
        Option renewer = new OptionBuilder("renewer").hasArg(true).argName("renewer")
                .desc("Specify delegation token renewer for kerberos enabled. " +
                        "Default value is yarn, sometimes you may change it equals to yarn.resourcemanager.principal").build();
        options.addOption(name);
        options.addOption(queue);
        options.addOption(cpu);
        options.addOption(memory);
        options.addOption(ammemory);
        options.addOption(instance);
        options.addOption(files);
        options.addOption(command);
        options.addOption(amEnv);
        options.addOption(executorEnv);
        options.addOption(amJars);
        options.addOption(principal);
        options.addOption(keytab);
        options.addOption(constraints);
        options.addOption(proxyUser);
        options.addOption(retry);
        options.addOption(javaOptions);
        options.addOption(renewer);
        return options;
    }

    static void checkArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkRequiredOption(commandLine, "cmd", "name", "am");
    }

    static ClientArguments toClientArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkArguments(commandLine);

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
        clientArguments.setAmMemory(Integer.parseInt(commandLine.getOptionValue("ammemory", "1024")));
        clientArguments.setInstances(Integer.parseInt(commandLine.getOptionValue("instance", "1")));
        clientArguments.setPrincipal(commandLine.getOptionValue("principal", null));
        clientArguments.setKeytab(commandLine.getOptionValue("keytab", null));
        clientArguments.setConstraints(commandLine.getOptionValue("constraints", null));
        clientArguments.setUser(commandLine.getOptionValue("user", null));
        clientArguments.setRetryThreshold(Integer.parseInt(commandLine.getOptionValue("retry", "1000")));
        clientArguments.setDelegationTokenRenewer(commandLine.getOptionValue("renewer", "yarn"));
        // env for executor
        Properties envsProps = commandLine.getOptionProperties("E");
        Map<String, String> executorEnvs = new HashMap<>();
        if (envsProps != null) {
            for (Map.Entry<Object, Object> item: envsProps.entrySet()) {
                executorEnvs.put(item.getKey().toString(), item.getValue().toString());
            }
        }
        clientArguments.setExecutorEnvironments(executorEnvs);

        // env for application master
        Properties amEnvsProps = commandLine.getOptionProperties("e");
        Map<String, String> amEnvs = new HashMap<>();
        if (amEnvsProps != null) {
            for (Map.Entry<Object, Object> item: amEnvsProps.entrySet()) {
                amEnvs.put(item.getKey().toString(), item.getValue().toString());
            }
        }
        clientArguments.setaMEnvironments(amEnvs);

        String[] files = commandLine.getOptionValues("file");
        if (files != null) {
            List<Artifact> artifacts = new ArrayList<>();
            for (String file : files) {
                Artifact artifact = fromString(file);
                if (artifact != null) {
                    artifacts.add(artifact);
                }
            }
            clientArguments.setArtifacts(artifacts);
        }
        String[] javaOptions = commandLine.getOptionValues("opt");
        if (javaOptions != null) {
            clientArguments.setJavaOptions(Arrays.asList(javaOptions));
        }
        return clientArguments;
    }

    private static final Pattern pattern = Pattern.compile("(?<path>[^#@]+)(?<ct>#[^@]+)?(?<hp>@.+)?$");

    /**
     * extract Artifact info from file string
     * schema://path[#compress_target][@hadoop_path]
     * @param file
     * @return
     */
    public static Artifact fromString(String file) {
        Matcher matcher = pattern.matcher(file);
        if (matcher.find()) {
            String path = matcher.group("path");
            String ct = matcher.group("ct");
            String hp = matcher.group("hp");
            Artifact artifact = new Artifact();
            if (ct != null) {
                artifact.setType(LocalResourceType.ARCHIVE);
                artifact.setLocalPath(path + ct);
            } else {
                if (path.endsWith(".tar.gz") || path.endsWith(".tgz") || path.endsWith(".zip")) {
                    artifact.setType(LocalResourceType.ARCHIVE);
                } else {
                    artifact.setType(LocalResourceType.FILE);
                }
                artifact.setLocalPath(path);
            }
            if (hp != null) {
                artifact.setHadoopConfDir(hp.substring(1));
            }
            return artifact;
        }
        return null;
    }

    static void printHelp() {
        StdIOUtil.printlnF("Action \"%s\" submit a application%n", Cli.ACTION_SUBMIT);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_SUBMIT);
        formatter.printHelp(syntax, buildOptions());
        StdIOUtil.println();
    }
}
