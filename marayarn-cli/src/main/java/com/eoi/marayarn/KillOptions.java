package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class KillOptions extends CliOptions {

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
        StdIOUtil.printlnF("Action \"%s\" kill a application %n", Cli.ACTION_KILL);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_KILL);
        formatter.printHelp(syntax, buildOptions());
        StdIOUtil.println();
    }
}