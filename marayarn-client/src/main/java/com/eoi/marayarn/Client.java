package com.eoi.marayarn;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.eoi.marayarn.Constants.*;

public class Client {
    public static final Logger logger = LoggerFactory.getLogger(Client.class);
    // Memory needed to launch the ApplicationMaster
    private static final int AM_MIN_MEMEORY = 256;
    // Core needed to launch the ApplicationMaster
    private static final int AM_MIN_CORE = 1;
    private static final FsPermission STAGING_DIR_PERMISSION =
            FsPermission.createImmutable(Short.parseShort("755", 8));
    private static final FsPermission APP_FILE_PERMISSION =
            FsPermission.createImmutable(Short.parseShort("644", 8));
    private static final String STAGE_DIR = ".stage";
    private static final String AM_JAR_KEY = "__marayarn_am__.jar";
    private static final Pattern fragment = Pattern.compile("#.*$");

    private final YarnClient yarnClient;
    protected final YarnConfiguration yarnConfiguration;

    public Client() {
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnConfiguration = new YarnConfiguration();
    }

    /**
     * launch an application
     * @param arguments arguments that tell how to start the application
     * @return ApplicationReport
     * @throws Exception ResourceLimitException: resource not available
     */
    public ApplicationReport launch(ClientArguments arguments) throws Exception {
        checkArguments(arguments);
        return submitApplication(arguments);
    }

    /**
     * check and set some default value for the arguments,
     * @param arguments client argument
     * @throws InvalidClientArgumentException
     */
    public void checkArguments(ClientArguments arguments) throws InvalidClientArgumentException {
        arguments.check();
    }

    protected void initConfiguration(ClientArguments arguments) {
        // check and set hadoopConfDir
        String[] possibleHadoopConfPaths = new String[3];
        if (arguments.getHadoopConfDir() != null && !arguments.getHadoopConfDir().isEmpty()) {
            possibleHadoopConfPaths[0] = arguments.getHadoopConfDir();
        } else {
            String hadoopConfiDir = System.getenv("HADOOP_CONF_DIR");
            String hadoopHome = System.getenv("HADOOP_HOME");
            if (hadoopConfiDir != null && !hadoopConfiDir.isEmpty()) {
                possibleHadoopConfPaths[1] = hadoopConfiDir;
            } else if (hadoopHome != null && !hadoopHome.isEmpty()) {
                possibleHadoopConfPaths[1] = hadoopHome + "/conf";
                possibleHadoopConfPaths[2] = hadoopHome + "/etc/conf";
            }
        }
        for (String possibleHadoopConfPath: possibleHadoopConfPaths) {
            if (possibleHadoopConfPath != null) {
                File dir = new File(possibleHadoopConfPath);
                try {
                    File[] files = FileUtil.listFiles(dir);
                    for (File file: files) {
                        if (file.isFile() && file.canRead() && file.getName().endsWith(".xml")) {
                            this.yarnConfiguration.addResource(new Path(file.getAbsolutePath()));
                        }
                    }
                } catch (Exception ex) {
                    logger.warn("Failed to list or add resource in {}", dir, ex);
                }
            }
        }
        // TODO: add more fs impl to prevent ServiceLoader file override?
        this.yarnConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    protected ApplicationReport submitApplication(ClientArguments arguments) throws Exception {
        initConfiguration(arguments);
        this.yarnClient.init(this.yarnConfiguration);
        this.yarnClient.start();
        logger.info("Started yarn client and creating application");
        YarnClientApplication newApp = this.yarnClient.createApplication();
        GetNewApplicationResponse newAppResponse = newApp.getNewApplicationResponse();
        logger.info("Created application: {}, maxResourceCapability: {}",
                newAppResponse.getApplicationId(), newAppResponse.getMaximumResourceCapability());
        // Verify whether the cluster has enough resources for our am and executor
        verifyAMResource(newAppResponse, arguments);
        logger.info("Setting up container launch context for our AM");
        checkYarnQueues(this.yarnClient, arguments.getQueue());
        ContainerLaunchContext amLaunchContext = createAmContainerLaunchContext(newAppResponse.getApplicationId(), arguments);
        // prepare and submit am
        ApplicationSubmissionContext submissionContext = newApp.getApplicationSubmissionContext();
        submissionContext.setApplicationName(arguments.getApplicationName());
        if (arguments.getQueue() != null && !arguments.getQueue().isEmpty()) {
            submissionContext.setQueue(arguments.getQueue());
        }
        submissionContext.setAMContainerSpec(amLaunchContext);
        submissionContext.setApplicationType("MARAYARN");
        // submissionContext.setMaxAppAttempts();
        submissionContext.setQueue(arguments.getQueue());
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(AM_MIN_MEMEORY);
        capability.setVirtualCores(AM_MIN_CORE);
        submissionContext.setResource(capability);
        logger.info("Submitting application {}", newAppResponse.getApplicationId());
        this.yarnClient.submitApplication(submissionContext);
        return this.yarnClient.getApplicationReport(submissionContext.getApplicationId());
    }

    private void checkYarnQueues(YarnClient yarnClient, String targetQueue) {
        try {
            List<QueueInfo> queues = yarnClient.getAllQueues();
            if (!queues.isEmpty() && targetQueue != null) { // check only if there are queues configured in yarn and for this session.
                boolean queueFound = false;
                StringBuilder queueNames = new StringBuilder();
                for (QueueInfo queue : queues) {
                    if (queue.getQueueName().equals(targetQueue)) {
                        queueFound = true;
                        break;
                    }
                    if(queueNames.length() > 0) {
                        queueNames.append(", ");
                    }
                    queueNames.append(queue.getQueueName());
                }
                if (!queueFound) {
                    logger.warn("The specified queue '{}' does not exist. Available queues: {}", targetQueue, queueNames.toString());
                }
            } else {
                logger.debug("The YARN cluster does not have any queues configured");
            }
        } catch (Throwable e) {
            logger.warn("Error while getting queue information from YARN", e);
            if (logger.isDebugEnabled()) {
                logger.debug("Error details", e);
            }
        }
    }

    // TODO: the following configuration will affect the final resource allocation
    //   yarn.scheduler.minimum-allocation-mb, yarn.scheduler.increment-allocation-mb
    //   yarn.scheduler.minimum-allocation-vcores, yarn.scheduler.increment-allocation-vcores
    private void verifyAMResource(GetNewApplicationResponse applicationResponse, ClientArguments arguments)
            throws ResourceLimitException{
        int memoryCapability = applicationResponse.getMaximumResourceCapability().getMemory();
        int coreCapability = applicationResponse.getMaximumResourceCapability().getVirtualCores();
        if (AM_MIN_MEMEORY + arguments.getInstances() * arguments.getMemory() > memoryCapability) { // 256 for AM
            throw new ResourceLimitException();
        }
        if (AM_MIN_CORE + arguments.getInstances() * arguments.getCpu() > coreCapability) {
            throw new ResourceLimitException();
        }
    }

    protected Map<String, LocalResource> prepareLocalResource(ApplicationId applicationId, ClientArguments arguments) throws Exception {
        FileSystem fs = FileSystem.get(this.yarnConfiguration);
        Path dst = new Path(fs.getHomeDirectory(), STAGE_DIR + "/" + applicationId.toString());
        Map<String, LocalResource> localResources = new HashMap<>();
        FileSystem.mkdirs(fs, dst, new FsPermission(STAGING_DIR_PERMISSION));
        // 上传AM
        Path src = new Path(arguments.getApplicationMasterJar());
        Path destPath = copyFileToRemote(dst, src);
        addLocalResource(localResources, AM_JAR_KEY, destPath, LocalResourceType.FILE, LocalResourceVisibility.APPLICATION);
        // 上传自定义资源
        for (Artifact artifact: arguments.getArtifacts()) {
            String localPath = artifact.getLocalPath();
            Path sFile = new Path(artifact.getLocalPath());
            Matcher fragmentMatcher = fragment.matcher(localPath);
            String fragment = sFile.getName();
            if (fragmentMatcher.find()) {
                fragment = fragmentMatcher.group(0).substring(1);
                String removeFragment = fragmentMatcher.replaceFirst("");
                sFile = new Path(removeFragment);
            }
            Path dFile = copyFileToRemote(dst, sFile);
            addLocalResource(localResources, fragment, dFile, artifact.getType(), artifact.getVisibility());
        }
        return localResources;
    }

    private URI fromHadoopURLToURI(URL url) {
        if (url == null)
            return null;
        try {
            return new URI(url.getScheme(), url.getUserInfo(), url.getHost(), url.getPort(), url.getFile(), null ,null);
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    protected Map<String, String> setupLaunchEnv(ClientArguments arguments, Map<String, LocalResource> artifacts) {
        Map<String, String> env = new HashMap<>();
        // setup command line
        env.put(AM_ENV_COMMANDLINE, arguments.getCommand());
        // setup class path
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$());
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append("./*");
        for (String c : this.yarnConfiguration.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        env.put(ApplicationConstants.Environment.CLASSPATH.toString(), classPathEnv.toString());
        // setup custom environment
        env.putAll(arguments.getaMEnvironments());
        // setup executor environment with EXECUTOR_LAUNCH_ENV_PREFIX
        for (Map.Entry<String, String> entry: arguments.getExecutorEnvironments().entrySet()) {
            env.put(EXECUTOR_LAUNCH_ENV_PREFIX + entry.getKey(), entry.getValue());
        }
        // setup environments for artifacts
        if (artifacts != null && artifacts.size() > 0) {
            List<String> keys = new ArrayList<>();
            List<String> files = new ArrayList<>();
            List<String> timestamps = new ArrayList<>();
            List<String> sizes = new ArrayList<>();
            List<String> visibilities = new ArrayList<>();
            List<String> types = new ArrayList<>();
            for (Map.Entry<String, LocalResource> artifact: artifacts.entrySet()) {
                if (artifact.getKey().equals(AM_JAR_KEY))
                    continue;
                keys.add(artifact.getKey());
                files.add(fromHadoopURLToURI(artifact.getValue().getResource()).toString());
                timestamps.add(String.format("%d", artifact.getValue().getTimestamp()));
                sizes.add(String.format("%d", artifact.getValue().getSize()));
                visibilities.add(artifact.getValue().getVisibility().toString());
                types.add(artifact.getValue().getType().toString());
            }
            env.put(EXECUTOR_ARTIFACTS_FILE_KEYS, String.join(",", keys));
            env.put(EXECUTOR_ARTIFACTS_FILES, String.join(",", files));
            env.put(EXECUTOR_ARTIFACTS_TIMESTAMPS, String.join(",", timestamps));
            env.put(EXECUTOR_ARTIFACTS_SIZES, String.join(",", sizes));
            env.put(EXECUTOR_ARTIFACTS_VISIBILITIES, String.join(",", visibilities));
            env.put(EXECUTOR_ARTIFACTS_TYPES, String.join(",", types));
        }
        return env;
    }

    protected List<String> prepareApplicationMasterCommands(ClientArguments arguments) {
        List<String> commands = new ArrayList<>();
        commands.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        commands.add("-server");
        commands.add("-Xmx" + AM_MIN_MEMEORY + "m");
        Path tmpDir = new Path(ApplicationConstants.Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
        commands.add("-Djava.io.tmpdir=" + tmpDir);
        // TODO support custom java options
        commands.add(arguments.getApplicationMasterClass());// main class
        commands.add("--executors");
        commands.add(String.format("%d", arguments.getInstances()));
        commands.add("--cores");
        commands.add(String.format("%d", arguments.getCpu()));
        commands.add("--memory");
        commands.add(String.format("%d", arguments.getMemory()));
        if (arguments.getQueue() != null && !arguments.getQueue().isEmpty()) {
            commands.add("--queue");
            commands.add(arguments.getQueue());
        }
        // ApplicationMaster arguments
        commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        return commands;
    }

    protected ContainerLaunchContext createAmContainerLaunchContext(ApplicationId appId, ClientArguments arguments) throws Exception {
        logger.info("Preparing local resource");
        Map<String, LocalResource> localResource = prepareLocalResource(appId, arguments);
        logger.info("Setup launch environment");
        Map<String, String> envs = setupLaunchEnv(arguments ,localResource);
        logger.info("Preparing commands for application master");
        List<String> commands = prepareApplicationMasterCommands(arguments);
        ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
        clc.setLocalResources(localResource);
        clc.setEnvironment(envs);
        clc.setCommands(commands);

        logger.info("===============================================================================");
        logger.info("Yarn AM launch context:");
        logger.info("    env:");
        for (Map.Entry<String, String> env: envs.entrySet()) {
            logger.info("        {} -> {}", env.getKey(), env.getValue());
        }
        logger.info("    resources:");
        for (Map.Entry<String, LocalResource> env: localResource.entrySet()) {
            logger.info("        {} -> {}", env.getKey(), env.getValue());
        }
        logger.info("    command:");
        logger.info("        {}", String.join(" ", commands));
        logger.info("===============================================================================");
        return clc;
    }

    /**
     * add key -> destPath to localResourceMap
     * @param localResourceMap
     * @param key
     * @param destPath
     * @param type
     */
    private void addLocalResource(Map<String, LocalResource> localResourceMap,
                                  String key, Path destPath, LocalResourceType type, LocalResourceVisibility visibility) {
        try {
            FileSystem fs = FileSystem.get(destPath.toUri(), this.yarnConfiguration);
            FileStatus destStatus = fs.getFileStatus(destPath);
            LocalResource localResource = Records.newRecord(LocalResource.class);
            localResource.setType(type);
            localResource.setVisibility(visibility);
            localResource.setResource(ConverterUtils.getYarnUrlFromPath(destPath));
            localResource.setTimestamp(destStatus.getModificationTime());
            localResource.setSize(destStatus.getLen());
            localResourceMap.put(key, localResource);
        } catch (IOException e) {
            logger.warn("Failed to add resource {}", destPath, e);
        }
    }

    private Path copyFileToRemote(Path destDir, Path srcPath) throws Exception {
        // the reason why don't use Path.getFileSystem is that
        // Path.toUri() will not decode the encoding stuff
        // FileSystem destFs = destDir.getFileSystem(this.yarnConfiguration);
        // FileSystem srcFs = srcPath.getFileSystem(this.yarnConfiguration);
        FileSystem destFs = FileSystem.get(new URI(destDir.toString()), this.yarnConfiguration);
        FileSystem srcFs = FileSystem.get(new URI(srcPath.toString()), this.yarnConfiguration);
        Path destPath = srcPath;
        if (!compareFs(srcFs, destFs)) {
            destPath = new Path(destDir, srcPath.getName());
            logger.info("Uploading resource {} -> {}", srcPath, destPath);
            FileUtil.copy(srcFs, srcPath, destFs, destPath, false, this.yarnConfiguration);
            destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION));
        } else {
            logger.info("Source and destination file systems are the same. Not copying {}", srcPath);
        }
        // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
        // version shows the specific version in the distributed cache configuration
        Path qualifiedDestPath = destFs.makeQualified(destPath);
        FileContext fc = FileContext.getFileContext(qualifiedDestPath.toUri(), this.yarnConfiguration);
        return fc.resolvePath(qualifiedDestPath);
    }

    /**
     * Return whether the two file systems are the same.
     */
    private boolean compareFs(FileSystem srcFS, FileSystem destFs) throws UnknownHostException {
        URI srcUri = srcFS.getUri();
        URI dstUri = destFs.getUri();
        if (srcUri.getScheme() == null || !srcUri.getScheme().equals(dstUri.getScheme())) {
            return false;
        }

        String srcHost = srcUri.getHost();
        String dstHost = dstUri.getHost();

        // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
        // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
        // match.
        if (srcHost != null && dstHost != null && !srcHost.equals(dstHost)) {
            srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
            dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
        }

        return srcHost.equals(dstHost) && srcUri.getPort() == dstUri.getPort();
    }
}
