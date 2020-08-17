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

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.eoi.marayarn.Constants.*;

public class Client {
    public static final Logger logger = LoggerFactory.getLogger(Client.class);
    private static final int AM_MIN_MEMEORY = 256;
    private static final FsPermission STAGING_DIR_PERMISSION =
            FsPermission.createImmutable(Short.parseShort("755", 8));
    private static final FsPermission APP_FILE_PERMISSION =
            FsPermission.createImmutable(Short.parseShort("644", 8));
    private static final String STAGE_DIR = ".stage";
    private static final String AM_JAR_KEY = "__marayarn_am__.jar";
    private static final Pattern fragment = Pattern.compile("#.*$");

    private final YarnClient yarnClient;
    private final YarnConfiguration yarnConfiguration;

    public Client() {
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnConfiguration = new YarnConfiguration();
    }

    public ApplicationReport launch(ClientArguments arguments) throws Exception {
        checkArguments(arguments);
        return submitApplication(arguments);
    }

    public void checkArguments(ClientArguments arguments) throws InvalidClientArgumentException {
        arguments.check();
    }

    public ApplicationReport submitApplication(ClientArguments arguments) throws Exception {
        this.yarnClient.init(this.yarnConfiguration);
        this.yarnClient.start();
        logger.info("Started yarn client and creating application");
        YarnClientApplication newApp = this.yarnClient.createApplication();
        GetNewApplicationResponse newAppResponse = newApp.getNewApplicationResponse();
        logger.info("Created application: {}, maxResourceCapability: {}",
                newAppResponse.getApplicationId(), newAppResponse.getMaximumResourceCapability());
        // Verify whether the cluster has enough resources for our AM
        verifyAMResource(newAppResponse);
        logger.info("Setting up container launch context for our AM");
        ContainerLaunchContext amLaunchContext = createAmContainerLaunchContext(newAppResponse, arguments);
        // prepare and submit am
        ApplicationSubmissionContext submissionContext = newApp.getApplicationSubmissionContext();
        submissionContext.setApplicationName(arguments.getApplicationName());
        if (arguments.getQueue() != null && !arguments.getQueue().isEmpty()) {
            submissionContext.setQueue(arguments.getQueue());
        }
        submissionContext.setAMContainerSpec(amLaunchContext);
        submissionContext.setApplicationType("MARAYARN");
        // submissionContext.setMaxAppAttempts();
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(AM_MIN_MEMEORY);
        capability.setVirtualCores(1);
        submissionContext.setResource(capability);
        logger.info("Submitting application {}", newAppResponse.getApplicationId());
        this.yarnClient.submitApplication(submissionContext);
        return this.yarnClient.getApplicationReport(submissionContext.getApplicationId());
    }

    private void verifyAMResource(GetNewApplicationResponse applicationResponse) throws ResourceLimitException{
        int memoryCapability = applicationResponse.getMaximumResourceCapability().getMemory();
        if (AM_MIN_MEMEORY > memoryCapability) { // 256 for AM
            throw new ResourceLimitException();
        }
    }

    private Map<String, LocalResource> prepareLocalResource(ApplicationId applicationId, ClientArguments arguments) throws Exception {
        FileSystem fs = FileSystem.get(this.yarnConfiguration);
        Path dst = new Path(fs.getHomeDirectory(), STAGE_DIR + "/" + applicationId.toString());
        Map<String, LocalResource> localResources = new HashMap<>();
        FileSystem.mkdirs(fs, dst, new FsPermission(STAGING_DIR_PERMISSION));
        // 上传AM
        Path src = new Path(arguments.getApplicationMasterJar());
        Path destPath = copyFileToRemote(dst, src);
        addLocalResource(localResources, AM_JAR_KEY, destPath, LocalResourceType.FILE);
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
            addLocalResource(localResources, fragment, dFile, artifact.getType());
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

    private Map<String, String> setupLaunchEnv(ApplicationId applicationId, ClientArguments arguments, Map<String, LocalResource> artifacts) {
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
        env.putAll(arguments.getAMEnvironments());
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

    private List<String> prepareApplicationMasterCommands(ApplicationId applicationId, ClientArguments arguments) {
        List<String> commands = new ArrayList<>();
        commands.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        commands.add("-server");
        commands.add("-Xmx" + AM_MIN_MEMEORY + "m");
        Path tmpDir = new Path(ApplicationConstants.Environment.PWD.$$(), this.yarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
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

    private ContainerLaunchContext createAmContainerLaunchContext(GetNewApplicationResponse appResponse, ClientArguments arguments) throws Exception {
        ApplicationId appId = appResponse.getApplicationId();
        logger.info("Preparing local resource");
        Map<String, LocalResource> localResource = prepareLocalResource(appId, arguments);
        logger.info("Setup launch environment");
        Map<String, String> envs = setupLaunchEnv(appId, arguments ,localResource);
        logger.info("Preparing commands for application master");
        List<String> commands = prepareApplicationMasterCommands(appId, arguments);
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
                                  String key, Path destPath, LocalResourceType type) {
        try {
            FileSystem fs = FileSystem.get(destPath.toUri(), this.yarnConfiguration);
            FileStatus destStatus = fs.getFileStatus(destPath);
            LocalResource localResource = Records.newRecord(LocalResource.class);
            localResource.setType(type);
            localResource.setVisibility(LocalResourceVisibility.APPLICATION);
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
