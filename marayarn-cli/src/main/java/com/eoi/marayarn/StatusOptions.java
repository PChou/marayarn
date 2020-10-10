package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class StatusOptions extends CliOptions {

    static Options buildOptions() {
        Options options = new Options();
        Option app = new OptionBuilder("app").hasArg(true).argName("app").required()
                .desc("Set application id of the Application").build();
        options.addOption(app);
        return options;
    }

    static void checkArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkRequiredOption(commandLine, "app");
    }

    static ClientArguments toClientArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkArguments(commandLine);

        ClientArguments clientArguments = new ClientArguments();
        String id = commandLine.getOptionValue("app");
        clientArguments.setApplicationId(id);
        return clientArguments;
    }

    static void printHelp() {
        System.out.printf("Action \"%s\" get status of application \n", Cli.ACTION_STATUS);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_STATUS);
        formatter.printHelp(syntax, buildOptions());
    }
}
