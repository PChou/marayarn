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

}
