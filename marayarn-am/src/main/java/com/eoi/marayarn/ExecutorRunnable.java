package com.eoi.marayarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.eoi.marayarn.Constants.*;

import java.net.URI;
import java.util.*;

public class ExecutorRunnable implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(ExecutorRunnable.class);
    private Configuration conf;
    private final Container container;
    private String commandLine;
    private NMClient nmClient;

    public ExecutorRunnable(Configuration configuration, Container container, String commandLine, NMClient nmClient) {
        this.conf = configuration;
        this.container = container;
        this.commandLine = commandLine;
        this.nmClient = nmClient;
    }

    private LocalResource setupLocalResource(
            Map<String, LocalResource> localResources,
            String key,
            String file,
            String timestamp,
            String size,
            String visibility,
            String type) {
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
        try {
            URI uri = new URI(file);
            amJarRsrc.setType(LocalResourceType.valueOf(type));
            amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(visibility));
            amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri));
            amJarRsrc.setTimestamp(Long.parseLong(timestamp));
            amJarRsrc.setSize(Long.parseLong(size));
            localResources.put(key, amJarRsrc);
        } catch (Exception ex) {
            logger.warn("Failed to prepare local resource {}", file, ex);
        }
        return amJarRsrc;
    }

    private List<String> prepareCommandLine(String commandLine) {
        List<String> commands = new ArrayList<>();
        commands.add(commandLine);
        commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        return commands;
    }

    private Map<String, LocalResource> prepareLocalResources() {
        Map<String, LocalResource> localResources = new HashMap<>();
        if (System.getenv(EXECUTOR_ARTIFACTS_FILES) == null || System.getenv(EXECUTOR_ARTIFACTS_FILES).isEmpty()) {
            return localResources;
        }
        String[] keys = System.getenv(EXECUTOR_ARTIFACTS_FILE_KEYS).split(",");
        String[] files = System.getenv(EXECUTOR_ARTIFACTS_FILES).split(",");
        String[] timestamps = System.getenv(EXECUTOR_ARTIFACTS_TIMESTAMPS).split(",");
        String[] sizes = System.getenv(EXECUTOR_ARTIFACTS_SIZES).split(",");
        String[] visibilities = System.getenv(EXECUTOR_ARTIFACTS_VISIBILITIES).split(",");
        String[] types = System.getenv(EXECUTOR_ARTIFACTS_TYPES).split(",");
        for (int i = 0; i < files.length; i++) {
            setupLocalResource(localResources, keys[i], files[i], timestamps[i], sizes[i], visibilities[i], types[i]);
        }
        return localResources;
    }

    private Map<String, String> prepareEnvironments() {
        Map<String, String> all = System.getenv();
        Map<String, String> executorEnvs = new HashMap<>();
        for (Map.Entry<String, String> entry: all.entrySet()) {
            if (entry.getKey().startsWith(EXECUTOR_LAUNCH_ENV_PREFIX)) {
                executorEnvs.put(entry.getKey().substring(EXECUTOR_LAUNCH_ENV_PREFIX.length()), entry.getValue());
            }
        }
        return executorEnvs;
    }

    @Override
    public void run() {
        logger.info("Setting up ContainerLaunchContext");
        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        List<String> commands = prepareCommandLine(commandLine);
        logger.info("Executor launching commands {}", String.join(" ", commands));
        context.setCommands(commands);
        Map<String, LocalResource> localResource = prepareLocalResources();
        if (localResource.size() > 0) {
            logger.info("Executor resources:");
            for (Map.Entry<String, LocalResource> env: localResource.entrySet()) {
                logger.info("        {} -> {}", env.getKey(), env.getValue());
            }
        }
        context.setLocalResources(localResource);
        Map<String, String> executorEnvs = prepareEnvironments();
        if (executorEnvs.size() > 0) {
            logger.info("Executor environments:");
            for (Map.Entry<String, String> env: executorEnvs.entrySet()) {
                logger.info("        {} -> {}", env.getKey(), env.getValue());
            }
        }
        context.setEnvironment(executorEnvs);
        try {
            nmClient.startContainer(container, context);
        } catch(Exception ex) {
            logger.error("Exception while starting container {}", container, ex);
        }
    }
}
