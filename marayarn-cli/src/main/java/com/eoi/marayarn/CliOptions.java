package com.eoi.marayarn;

import org.apache.commons.cli.CommandLine;

import java.util.ArrayList;
import java.util.List;

public class CliOptions {

    static void checkRequiredOption(CommandLine commandLine, String... arguments) throws InvalidCliArgumentException {
        List<String> missingOptions = new ArrayList<>();
        for(String argument : arguments) {
            if (!commandLine.hasOption(argument)) {
                missingOptions.add(argument);
            }
        }
        if(!missingOptions.isEmpty()) {
            String missingStr = String.format("Missing required argument: [%s]",
                    String.join(",", missingOptions)
            );
            throw new InvalidCliArgumentException(missingStr);
        }
    }

    static void checkMustOneOption(CommandLine commandLine, String... arguments) throws InvalidCliArgumentException {
        int count = 0;
        for(String argument : arguments) {
            if (!commandLine.hasOption(argument)) {
                count++;
            }
        }
        if(count < 1) {
            String missingStr = String.format("Must provide one argument at least: [%s]",
                    String.join(",", arguments)
            );
            throw new InvalidCliArgumentException(missingStr);
        }
    }
}
