package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class InfoOptions extends CliOptions  {

    static Options buildOptions() {
        Options options = new Options();
        Option url = new OptionBuilder("url").hasArg(true).argName("url").required()
                .desc("Set tracking url of the Application").build();
        options.addOption(url);
        return options;
    }

    static void checkArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkRequiredOption(commandLine, "url");
    }

    static String getUrl(CommandLine commandLine) throws InvalidCliArgumentException {
        checkArguments(commandLine);

        return commandLine.getOptionValue("url");
    }

    static void printHelp() {
        System.out.printf("Action \"%s\" get information about application \n", Cli.ACTION_INFO);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_INFO);
        formatter.printHelp(syntax, buildOptions());
    }

}
