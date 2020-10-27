package com.eoi.marayarn;

import org.apache.commons.cli.*;

public class InfoOptions extends CliOptions  {

    static Options buildOptions() {
        Options options = new Options();
        Option app = new OptionBuilder("app").hasArg(true).argName("app").required()
                .desc("Set application id of the Application").build();
        Option principal = new OptionBuilder("principal").hasArg(true)
                .desc("Principal to be used to login to KDC, while running secure HDFS").build();
        Option keytab = new OptionBuilder("keytab").hasArg(true)
                .desc("The full path to the file that contains the keytab for the principal specified above").build();
        options.addOption(app);
        options.addOption(principal);
        options.addOption(keytab);
        return options;
    }

    static void checkArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkRequiredOption(commandLine, "app");
    }

    static ClientArguments toClientArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkArguments(commandLine);

        ClientArguments clientArguments = new ClientArguments();
        String app = commandLine.getOptionValue("app");
        clientArguments.setApplicationId(app);
        clientArguments.setPrincipal(commandLine.getOptionValue("principal", null));
        clientArguments.setKeytab(commandLine.getOptionValue("keytab", null));
        return clientArguments;
    }

    static void printHelp() {
        StdIOUtil.printlnF("Action \"%s\" get information about application %n", Cli.ACTION_INFO);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_INFO);
        formatter.printHelp(syntax, buildOptions());
        StdIOUtil.println();
    }

}
