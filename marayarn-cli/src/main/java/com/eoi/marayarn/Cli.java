package com.eoi.marayarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Cli {
    static final Logger logger = LoggerFactory.getLogger(Cli.class);

    // actions
    static final String ACTION_SUBMIT = "submit";
    static final String ACTION_STATUS = "status";
    static final String ACTION_KILL = "kill";

    public static void main(String[] args) {
        run(args);
    }

    static int run(String[] args) {
        // check for action
        if (args.length < 1) {
            printHelp();
            System.out.println();
            System.out.println("Please specify an action.");
            return 1;
        }

        // get action
        String action = args[0];
        // remove action from parameters
        final String[] params = Arrays.copyOfRange(args, 1, args.length);

        switch (action) {
            case ACTION_SUBMIT:
                submit(params);
                return 0;
            case ACTION_STATUS:
                status(params);
                return 0;
            case ACTION_KILL:
                kill(params);
                return 0;
            case "-h":
            case "--help":
                return 0;
            case "-v":
            case "--version":
                return 0;
            default:
                printHelp();
                System.out.println();
                System.out.printf("\"%s\" is not a valid action.\n", action);
                return 1;
        }

    }

    static void submit(String[] args) {
        Options options = SubmitOptions.buildOptions();
        CommandLineParser parser = new GnuParser();
        try(Client client = new Client()) {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("help")) {
                SubmitOptions.printHelp();
                System.exit(0);
            }
            ClientArguments clientArguments = SubmitOptions.toClientArguments(commandLine);
            ApplicationReport report = client.launch(clientArguments);
            logger.info("application id: {}", report.getApplicationId());
            logger.info("Tracking url: {}", report.getTrackingUrl());
        } catch (Exception ex) {
            SubmitOptions.printHelp();
            logger.error(String.format("Failed to run action \"%s\"", ACTION_SUBMIT), ex);
        }
    }

    static void status(String[] args) {
        Options options = StatusOptions.buildOptions();
        CommandLineParser parser = new GnuParser();
        try(Client client = new Client()) {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("help")) {
                StatusOptions.printHelp();
                System.exit(0);
            }
            ClientArguments clientArguments = StatusOptions.toClientArguments(commandLine);
            ApplicationReport report = client.get(clientArguments);
            logger.info("application id: {}", report.getApplicationId());
            logger.info("Tracking url: {}", report.getTrackingUrl());
            logger.info("application state: {}", report.getYarnApplicationState());
            logger.info("final state: {}", report.getFinalApplicationStatus());
        } catch (Exception ex) {
            StatusOptions.printHelp();
            logger.error(String.format("Failed to run action \"%s\"", ACTION_STATUS), ex);
        }
    }

    static void kill(String[] args) {
        Options options = KillOptions.buildOptions();
        CommandLineParser parser = new GnuParser();
        try(Client client = new Client()) {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("help")) {
                KillOptions.printHelp();
                System.exit(0);
            }
            ClientArguments clientArguments = KillOptions.toClientArguments(commandLine);
            client.kill(clientArguments);
            logger.info("killed application {}", clientArguments.getApplicationId());
        } catch (Exception ex) {
            KillOptions.printHelp();
            logger.error(String.format("Failed to run action \"%s\"", ACTION_KILL), ex);
        }
    }

    static void printHelp() {
        System.out.println("./marayarn <ACTION> [OPTIONS] [ARGUMENTS]");
        System.out.println();
        System.out.println("The following actions are available:");
        System.out.println();
        SubmitOptions.printHelp();
        System.out.println();
        KillOptions.printHelp();
        System.out.println();
        StatusOptions.printHelp();
    }
}
