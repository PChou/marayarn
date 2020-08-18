package com.eoi.marayarn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All arguments the Client accepted
 */
public class ClientArguments {
    /**
     * (Required) Marayarn ApplicationMaster的jar包所在路径URI
     * 支持file://, hdfs://, http://, ftp://表示的URI
     * Client会将文件传输到当前hdfs环境，如果目标URI跟当前hdfs是同一个环境，则会跳过上传步骤；
     * 其他格式的URI都会将文件上传到$HOME/.stage/$APPLICATION_ID/__marayarn_am__.jar
     */
    private String applicationMasterJar;
    /**
     * (Required) ApplicationName
     * 提交到yarn上的Application Name
     */
    private String applicationName;
    /**
     * ApplicationMasterClass的主类，内部指定为com.eoi.marayarn.MaraApplicationMaster
     * 见src/main/java/com/eoi/marayarn/MaraApplicationMaster.java
     */
    private String applicationMasterClass;
    /**
     * hadoop配置文件路径
     * 如果不指定的话，会依次尝试检查相关环境变量和CLassPath
     * 1. $HADOOP_CONF_DIR
     * 2. $HADOOP_HOME/conf
     * 3. $HADOOP_HOME/etc/conf  // hadoop 2.2
     * 4. java ClassPath
     */
    private String hadoopConfDir;
    /**
     * 设置yarn资源池的队列名，可不指定
     */
    private String queue;
    /**
     * 设置Executor的vcore, 默认为1
     */
    private int cpu;
    /**
     * 设置Executor的memory, 单位MB, 默认为512
     */
    private int memory;
    /**
     * 设置Executor的实例数量
     */
    private int instances;
    /**
     * TODO 设置用户
     */
    private String user;
    /**
     * 内部保留，设置ApplicationMaster的启动环境变量
     */
    private Map<String, String> aMEnvironments;
    /**
     * 设置Executor的启动环境变量
     */
    private Map<String, String> executorEnvironments;
    /**
     * 设置Executor需要的物料，Artifact的集合，Client会上传这些到hdfs
     * 支持file://, hdfs://, http://, ftp://表示的URI
     * Client会将文件传输到当前hdfs环境，如果目标URI跟当前hdfs是同一个环境，则会跳过上传步骤；
     * 其他格式的URI都会将文件上传到$HOME/.stage/$APPLICATION_ID/__marayarn_am__.jar
     * 支持使用归档文件(tar.gz, zip)，yarn会自动解压
     * URI最后的fragment部分会作为resource的key告知yarn，yarn在解压缩的时候，会以这个key作为外层目录
     * 例如:
     *    file:///Users/pchou/Downloads/logstash-7.3.0.tar.gz#dir
     *    http://host:9999/jaxjobjarlib/spark/logstash-7.3.0.tar.gz#dir
     *    都会上传logstash-7.3.0.tar.gz，解压后container的当前目录下有dir/logstash/...
     */
    private List<Artifact> artifacts;
    /**
     * (Required) 设置启动Executor的命令行
     * 例如:
     *    dir/logstash/bin/logstash -e 'input { stdin { } } output { stdout {} }'
     */
    private String command;

    public String getApplicationMasterJar() {
        return applicationMasterJar;
    }

    public void setApplicationMasterJar(String applicationMasterJar) {
        this.applicationMasterJar = applicationMasterJar;
    }

    public String getApplicationMasterClass() {
        return applicationMasterClass;
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public ClientArguments setHadoopConfDir(String hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
        return this;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public int getInstances() {
        return instances;
    }

    public void setInstances(int instances) {
        this.instances = instances;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Map<String, String> getaMEnvironments() {
        return aMEnvironments;
    }

    public ClientArguments setaMEnvironments(Map<String, String> aMEnvironments) {
        this.aMEnvironments = aMEnvironments;
        return this;
    }

    public Map<String, String> getExecutorEnvironments() {
        return executorEnvironments;
    }

    public void setExecutorEnvironments(Map<String, String> executorEnvironments) {
        this.executorEnvironments = executorEnvironments;
    }

    public List<Artifact> getArtifacts() {
        return artifacts;
    }

    public void setArtifacts(List<Artifact> artifacts) {
        this.artifacts = artifacts;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }


    private void checkIfNullOrEmpty(String t, String literal) throws InvalidClientArgumentException {
        if (t == null || t.isEmpty()) {
            throw new InvalidClientArgumentException("Invalid null or empty argument " + literal);
        }
    }

    public void check() throws InvalidClientArgumentException {
        if (this.applicationMasterClass == null || this.applicationMasterClass.isEmpty()) {
            this.applicationMasterClass = "com.eoi.marayarn.MaraApplicationMaster";
        }
        checkIfNullOrEmpty(this.applicationMasterJar, "ApplicationMasterJar");
        checkIfNullOrEmpty(this.applicationName, "ApplicationName");
        if (this.getCpu() <= 0) {
            this.setCpu(1);
        }
        if (this.getMemory() <= 0) {
            this.setMemory(512);
        }
        if (this.getInstances() <= 0) {
            this.setInstances(1);
        }
        checkIfNullOrEmpty(this.command, "Command");
        if (this.aMEnvironments == null) {
            this.aMEnvironments = new HashMap<>();
        }
        if (this.executorEnvironments == null) {
            this.executorEnvironments = new HashMap<>();
        }
        if (this.artifacts == null) {
            this.artifacts = new ArrayList<>();
        }
    }
}
