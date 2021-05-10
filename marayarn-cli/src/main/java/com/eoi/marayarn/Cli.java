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
    static final String ACTION_INFO = "info";
    static final String ACTION_SCALE = "scale";

    public static void main(String[] args) {
        run(args);
    }

    static int run(String[] args) {
        // check for action
        if (args.length < 1) {
            printHelp();
            StdIOUtil.printlnP("Please specify an action.");
            return 1;
        }

        // get action
        String action = args[0];
        // remove action from parameters
        final String[] params = Arrays.copyOfRange(args, 1, args.length);

        try {
            // do action
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
                case ACTION_INFO:
                    info(params);
                    return 0;
                case ACTION_SCALE:
                    scale(params);
                    return 0;
                case "-h":
                case "--help":
                    return 0;
                case "-v":
                case "--version":
                    return 0;
                default:
                    printHelp();
                    StdIOUtil.printlnFS("\"%s\" is not a valid action.%n", action);
                    return 1;
            }
        } catch (Exception ex) {
            printHelp();
            logger.error(String.format("Failed to run action \"%s\"", action), ex);
            return 1;
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Execute Actions
    // --------------------------------------------------------------------------------------------

    /**
     * Executions the submit action.
     * @param args Command line arguments for the submit action.
     * @throws Exception
     */
    static void submit(String[] args) throws Exception {
        Options options = SubmitOptions.buildOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        ClientArguments clientArguments = SubmitOptions.toClientArguments(commandLine);
        try(Client client = new Client()) {
            ApplicationReport report = client.launch(clientArguments);
            logger.info("application id: {}", report.getApplicationId());
            logger.info("tracking url: {}", report.getTrackingUrl());
        }
    }

    /**
     * Executions the status action.
     * @param args Command line arguments for the status action.
     * @throws Exception
     */
    static void status(String[] args) throws Exception {
        Options options = StatusOptions.buildOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        ClientArguments clientArguments = StatusOptions.toClientArguments(commandLine);
        try(Client client = new Client()) {
            ApplicationReport report = client.get(clientArguments);
            logger.info("application master host: {}", report.getHost());
            logger.info("application master rpc port: {}", report.getRpcPort());
            logger.info("application id: {}", report.getApplicationId());
            logger.info("tracking url: {}", report.getOriginalTrackingUrl());
            logger.info("application state: {}", report.getYarnApplicationState());
            logger.info("final status: {}", report.getFinalApplicationStatus());
            logger.info("diagnostics: {}", report.getDiagnostics());
            logger.info("queue: {}", report.getQueue());
            logger.info("user: {}", report.getUser());
            logger.info("start time: {}", report.getStartTime());
        }
    }

    /**
     * Executions the kill action.
     * @param args Command line arguments for the kill action.
     * @throws Exception
     */
    static void kill(String[] args) throws Exception {
        Options options = KillOptions.buildOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        ClientArguments clientArguments = KillOptions.toClientArguments(commandLine);
        try(Client client = new Client()) {
            client.kill(clientArguments);
            logger.info("action kill done");
        }
    }

    /**
     * Executions the info action.
     * @param args Command line arguments for the info action.
     * @throws Exception
     */
    static void info(String[] args) throws Exception {
        Options options = InfoOptions.buildOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        ClientArguments clientArguments = InfoOptions.toClientArguments(commandLine);
        String trackingUrl;
        try(Client client = new Client()) {
            ApplicationReport report = client.get(clientArguments);
            trackingUrl = report.getOriginalTrackingUrl();
        }
        try(AMClient client = new AMClient()) {
            ApplicationInfo info = client.getApplication(trackingUrl);
            String infoPrint = JsonUtil.print(info);
            logger.info("info: \n{}", infoPrint);
        }
    }

    /**
     * Executions the scale action.
     * @param args Command line arguments for the scale action.
     * @throws Exception
     */
    static void scale(String[] args) throws Exception {
        Options options = ScaleOptions.buildOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        ClientArguments clientArguments = ScaleOptions.toClientArguments(commandLine);
        String trackingUrl;
        try(Client client = new Client()) {
            ApplicationReport report = client.get(clientArguments);
            trackingUrl = report.getOriginalTrackingUrl();
        }
        ScaleRequest request = ScaleOptions.toClientRequest(commandLine);
        try(AMClient client = new AMClient()) {
            AMResponse response = client.scaleApplication(trackingUrl, request);
            String ack = JsonUtil.print(response);
            logger.info("ack: \n{}", ack);
            logger.info("action scale done");
        }
    }

    /**
     * Print the help of Command Line Arguments
     */
    static void printHelp() {
        StdIOUtil.printlnS("./marayarn <ACTION> [OPTIONS] [ARGUMENTS]");
        StdIOUtil.printlnS("The following actions are available:");
        SubmitOptions.printHelp();
        StatusOptions.printHelp();
        KillOptions.printHelp();
        InfoOptions.printHelp();
        ScaleOptions.printHelp();
    }
}
