package com.eoi.marayarn;

import org.apache.commons.cli.*;

public class ScaleOptions extends CliOptions {

    static Options buildOptions() {
        Options options = new Options();
        Option app = new OptionBuilder("app").hasArg(true).argName("app").required()
                .desc("Set application id of the Application").build();
        Option instance = new OptionBuilder("instance").hasArg(true).argName("int").required()
                .desc("The number of instance of the application (not include am)").build();
        options.addOption(app);
        options.addOption(instance);
        return options;
    }

    static void checkArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkRequiredOption(commandLine, "app", "instance");
    }

    static ClientArguments toClientArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkArguments(commandLine);

        ClientArguments clientArguments = new ClientArguments();
        String id = commandLine.getOptionValue("app");
        clientArguments.setApplicationId(id);
        return clientArguments;
    }

    static ScaleRequest toClientRequest(CommandLine commandLine) throws InvalidCliArgumentException {
        checkArguments(commandLine);
        ScaleRequest request = new ScaleRequest();
        Integer instances = Integer.parseInt(commandLine.getOptionValue("instance", "1"));
        request.setInstances(instances);
        return request;
    }

    static void printHelp() {
        StdIOUtil.printlnF("Action \"%s\" scale a application %n", Cli.ACTION_SCALE);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_SCALE);
        formatter.printHelp(syntax, buildOptions());
        StdIOUtil.println();
    }
}
