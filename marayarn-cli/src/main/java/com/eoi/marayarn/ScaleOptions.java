package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ScaleOptions extends CliOptions {

    static Options buildOptions() {
        Options options = new Options();
        Option url = new OptionBuilder("url").hasArg(true).argName("url").required()
                .desc("Set tracking url of the Application").build();
        Option instance = new OptionBuilder("instance").hasArg(true).argName("int").required()
                .desc("The number of instance of the application (not include am)").build();
        options.addOption(url);
        options.addOption(instance);
        return options;
    }

    static void checkArguments(CommandLine commandLine) throws InvalidCliArgumentException {
        checkRequiredOption(commandLine, "url", "instance");
    }

    static String getUrl(CommandLine commandLine) throws InvalidCliArgumentException {
        checkArguments(commandLine);

        return commandLine.getOptionValue("url");
    }

    static ScaleRequest toClientRequest(CommandLine commandLine) throws InvalidCliArgumentException {
        checkArguments(commandLine);
        ScaleRequest request = new ScaleRequest();
        Integer instances = Integer.parseInt(commandLine.getOptionValue("instance", "1"));
        request.setInstances(instances);
        return request;
    }

    static void printHelp() {
        System.out.printf("Action \"%s\" scale a application \n", Cli.ACTION_SCALE);
        HelpFormatter formatter = new HelpFormatter();
        String syntax = String.format("%s [OPTIONS]", Cli.ACTION_SCALE);
        formatter.printHelp(syntax, buildOptions());
    }
}
