package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class KillOptions extends CliOptions {

    static Options buildOptions() {
        Options options = new Options();
        Option help = new com.eoi.marayarn.OptionBuilder("help").hasArg(false)
                .desc("Print help").build();
        Option id = new com.eoi.marayarn.OptionBuilder("id").hasArg(true).argName("id").required()
                .desc("Set id of the Application").build();
        options.addOption(help);
        options.addOption(id);
        return options;
    }

    static ClientArguments toClientArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkRequiredOption(commandLine, "id");

        ClientArguments clientArguments = new ClientArguments();
        String id = commandLine.getOptionValue("id");
        clientArguments.setApplicationId(id);
        return clientArguments;
    }

    static void printHelp() {
        System.out.printf("Action \"%s\" delete a application \n", Cli.ACTION_KILL);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_KILL);
        formatter.printHelp(syntax, buildOptions());
    }
}