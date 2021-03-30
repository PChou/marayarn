package com.eoi.marayarn;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
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

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.eoi.marayarn.Constants.*;

public class Client implements Closeable {

    public static final Logger logger = LoggerFactory.getLogger(Client.class);
    // Memory needed to launch the ApplicationMaster
    private static final int AM_MIN_MEMEORY = 1024;
    // Core needed to launch the ApplicationMaster
    private static final int AM_MIN_CORE = 1;
    private static final FsPermission STAGING_DIR_PERMISSION =
            FsPermission.createImmutable(Short.parseShort("755", 8));
    private static final FsPermission APP_FILE_PERMISSION =
            FsPermission.createImmutable(Short.parseShort("644", 8));
    private static final String STAGE_DIR = ".stage";
    private static final String AM_JAR_KEY = "__marayarn_am__.jar";
    private static final String AM_KEY_TAB = "__kt__.keytab";
    private static final Pattern fragment = Pattern.compile("#.*$");

    private final YarnClient yarnClient;
    protected YarnConfiguration yarnConfiguration;

    public Client() {
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnConfiguration = new YarnConfiguration();
    }

    @Override
    public void close() throws IOException {
        this.yarnClient.close();
    }

    /**
     * launch an application
     * @param arguments arguments that tell how to start the application
     * @return ApplicationReport
     * @throws Exception ResourceLimitException: resource not available
     */
    public ApplicationReport launch(ClientArguments arguments) throws Exception {
        arguments.checkSubmit();
        return doActionWithUGI(arguments, () -> submitApplication(arguments));
    }

    /**
     * get an application
     * @param arguments arguments that tell how to get the application
     * @return ApplicationReport
     * @throws Exception
     */
    public ApplicationReport get(ClientArguments arguments) throws Exception {
        arguments.checkApp();
        return doActionWithUGI(arguments, () -> getApplication(arguments));
    }

    /**
     * kill an application
     * @param arguments arguments that tell how to kill the application
     * @throws Exception
     */
    public void kill(ClientArguments arguments) throws Exception {
        arguments.checkApp();
        doActionWithUGI(arguments, () -> killApplication(arguments));
    }

    private <T> T doActionWithUGI(ClientArguments arguments, ClientActionFunction<T> action) throws Exception {
        initConfiguration(arguments);
        UserGroupInformation.setConfiguration(this.yarnConfiguration);
        if (arguments.getPrincipal() != null && !arguments.getPrincipal().isEmpty()
                && arguments.getKeytab() != null && !arguments.getKeytab().isEmpty()) {
            UserGroupInformation ugi = null;
            try {
                logger.info("Login from {} with keytab {}", arguments.getPrincipal(), arguments.getKeytab());
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(arguments.getPrincipal(), arguments.getKeytab());
            } catch (IOException ex) {
                logger.warn("Failed to login user from keytab: {}({})", arguments.getPrincipal(), arguments.getKeytab());
            }
            if (ugi != null) {
                logger.info("Do action via user {}", ugi.getUserName());
                return ugi.doAs((PrivilegedExceptionAction<T>) () -> action.doAction());
            } else {
                throw new Exception(String.format("Failed to login user from keytab: %s(%s)", arguments.getPrincipal(), arguments.getKeytab()));
            }
        } else {
            UserGroupInformation proxyUser = null;
            if (arguments.getUser() != null && !arguments.getUser().isEmpty()) {
                proxyUser = UserGroupInformation.createProxyUser(arguments.getUser(),
                        UserGroupInformation.getCurrentUser());
            }
            if (proxyUser != null) {
                return proxyUser.doAs((PrivilegedExceptionAction<T>) () -> action.doAction());
            } else {
                return action.doAction();
            }
        }
    }

    private YarnConfiguration initConfigurationByPath(String specifiedHadoopConfPath) {
        YarnConfiguration configuration = new YarnConfiguration();
        String[] possibleHadoopConfPaths = new String[3];
        if (specifiedHadoopConfPath != null && !specifiedHadoopConfPath.isEmpty()) {
            possibleHadoopConfPaths[0] = specifiedHadoopConfPath;
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
                            configuration.addResource(new Path(file.getAbsolutePath()));
                        }
                    }
                } catch (Exception ex) {
                    logger.warn("Failed to list or add resource in {}", dir, ex);
                }
            }
        }
        // TODO: add more fs impl to prevent ServiceLoader file override?
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return configuration;
    }

    protected void initConfiguration(ClientArguments arguments) {
        this.yarnConfiguration = initConfigurationByPath(arguments.getHadoopConfDir());
    }

    protected ApplicationReport submitApplication(ClientArguments arguments) throws Exception {
        this.yarnClient.init(this.yarnConfiguration);
        this.yarnClient.start();
        logger.info("Started yarn client and creating application");
        List<NodeReport> nodeReports = this.yarnClient.getNodeReports();
        for (NodeReport nodeReport: nodeReports) {
            logger.info("node: {}, rack: {}, label: {}", nodeReport.getNodeId(), nodeReport.getRackName(), nodeReport.getNodeLabels());
        }
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
        submissionContext.setApplicationType(YARN_MARAYARN_APP_TYPE);
        submissionContext.setMaxAppAttempts(1);
        submissionContext.setQueue(arguments.getQueue());
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(AM_MIN_MEMEORY);
        capability.setVirtualCores(AM_MIN_CORE);
        submissionContext.setResource(capability);
        logger.info("Submitting application {}", newAppResponse.getApplicationId());
        this.yarnClient.submitApplication(submissionContext);
        return this.yarnClient.getApplicationReport(submissionContext.getApplicationId());
    }

    protected ApplicationReport getApplication(ClientArguments arguments) throws Exception {
        this.yarnClient.init(this.yarnConfiguration);
        this.yarnClient.start();
        ApplicationId applicationId = ApplicationId.fromString(arguments.getApplicationId());
        return this.yarnClient.getApplicationReport(applicationId);
    }

    protected Boolean killApplication(ClientArguments arguments) throws Exception {
        this.yarnClient.init(this.yarnConfiguration);
        this.yarnClient.start();
        ApplicationId applicationId = ApplicationId.fromString(arguments.getApplicationId());
        this.yarnClient.killApplication(applicationId);
        return true;
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
        Path destPath = copyFileToRemote(dst, src, null);
        addLocalResource(localResources, AM_JAR_KEY, destPath, LocalResourceType.FILE, LocalResourceVisibility.APPLICATION);
        // 上传keytab
        if (arguments.getPrincipal() != null && !arguments.getPrincipal().isEmpty()
                && arguments.getKeytab() != null && !arguments.getKeytab().isEmpty()) {
            Path keyTabSrc = new Path(arguments.getKeytab());
            Path keyTabDestPath = copyFileToRemote(dst, keyTabSrc, null);
            addLocalResource(localResources, AM_KEY_TAB, keyTabDestPath, LocalResourceType.FILE, LocalResourceVisibility.APPLICATION);
        }
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
            YarnConfiguration configuration = null;
            if (artifact.getHadoopConfDir() != null && !artifact.getHadoopConfDir().isEmpty()) {
                configuration = initConfigurationByPath(artifact.getHadoopConfDir());
            }
            Path dFile = copyFileToRemote(dst, sFile, configuration);
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

    protected Map<String, String> setupLaunchEnv
            (ClientArguments arguments, Map<String, LocalResource> artifacts) {
        Map<String, String> env = new HashMap<>();
        // setup command line
        // convert command line to base64 encoded string, so that prevent auto environment evaluation by yarn
        String safeCommandLine = Base64.getEncoder().encodeToString(arguments.getCommand().getBytes(StandardCharsets.UTF_8));
        env.put(AM_ENV_COMMANDLINE, safeCommandLine);
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
        if (!Utils.StringEmpty(arguments.getQueue())) {
            commands.add("--queue");
            commands.add(arguments.getQueue());
        }
        if (!Utils.StringEmpty(arguments.getConstraints()))  {
            commands.add("--constraints");
            commands.add(arguments.getConstraints());
        }
        if (!Utils.StringEmpty(arguments.getPrincipal()) && !Utils.StringEmpty(arguments.getKeytab())) {
            commands.add("--principal");
            commands.add(arguments.getPrincipal());
            commands.add("--keytab");
            commands.add(AM_KEY_TAB);
        }
        if (arguments.getRetryThreshold() != null) {
            commands.add("--retry");
            commands.add(String.format("%d", arguments.getRetryThreshold()));
        }
        // ApplicationMaster arguments
        commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        return commands;
    }

    protected ContainerLaunchContext createAmContainerLaunchContext(
            ApplicationId appId, ClientArguments arguments)
            throws Exception {
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
        if (arguments.getPrincipal() != null && !arguments.getPrincipal().isEmpty()
                && arguments.getKeytab() != null && !arguments.getKeytab().isEmpty()) {
            logger.info("Preparing for kerberos principal {}({}) and set hdfs delegation token", arguments.getPrincipal(), arguments.getKeytab());
            Credentials creds = UserGroupInformation.getCurrentUser().getCredentials();
            FileSystem defaultFS = FileSystem.get(yarnConfiguration);
            defaultFS.addDelegationTokens("yarn", creds);
            clc.setTokens(ByteBuffer.wrap(serializeCreds(creds)));
        }
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

    private Path copyFileToRemote(Path destDir, Path srcPath, YarnConfiguration srcConfiguration) throws Exception {
        // the reason why don't use Path.getFileSystem is that
        // Path.toUri() will not decode the encoding stuff
        // FileSystem destFs = destDir.getFileSystem(this.yarnConfiguration);
        // FileSystem srcFs = srcPath.getFileSystem(this.yarnConfiguration);
        FileSystem destFs = FileSystem.get(new URI(destDir.toString()), this.yarnConfiguration);
        FileSystem srcFs = FileSystem.get(new URI(srcPath.toString()), srcConfiguration == null ? this.yarnConfiguration : srcConfiguration);
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
    private boolean compareFs(FileSystem srcFS, FileSystem destFs) {
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
            try {
                srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
                dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
            } catch (UnknownHostException ex) {
                return false;
            }
        }

        return srcHost.equals(dstHost) && srcUri.getPort() == dstUri.getPort();
    }

    private byte[] serializeCreds(Credentials creds) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(byteStream);
        creds.writeTokenStorageToStream(dataStream);
        return byteStream.toByteArray();
    }

    @FunctionalInterface
    public interface ClientActionFunction<T> {
        T doAction() throws Exception;
    }
}
