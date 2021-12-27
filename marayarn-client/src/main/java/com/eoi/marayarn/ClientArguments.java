package com.eoi.marayarn;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All arguments the Client accepted
 */
public class ClientArguments {
    /**
     * (Required) yarn ApplicationId
     * 更新Application需要指定ApplicationId
     */
    private String applicationId;
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

    private YarnConfiguration configuration;
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
     * proxyUser
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
    /**
     * kerberos principal
     */
    private String principal;
    /**
     * kerberos keytab file local path
     */
    private String keytab;
    /**
     * like marathon Constraints
     * https://mesosphere.github.io/marathon/docs/constraints.html
     * 格式
     * field,operator[,value]
     * field 可以是node,rack
     * operator 支持CLUSTER,LIKE,UNLIKE,IS,UNIQUE,GROUP_BY
     * value一般是对应field的字符串或正则。对于CLUSTER, value可以不填;对于UNIQUE/GROUP_BY, value不需要填
     * ie.
     * node,CLUSTER: 随机选择一个node，所有实例部署到这个node上
     * node,LIKE,vm319[0-9]: 实例分布在满足node名称为vm319[0-9]的节点上
     * rack,IS,/rack1: 实例分布在/rack1的节点上
     * rack,UNIQUE: 实例在每种rack中只能有唯一的实例
     * node,UNIQUE: 实例在所有node上最多只有一个实例
     * node,CLUSTER,vm3195: 同IS, 实例全部分布在vm3195上
     * node,GROUP_BY: 实例按照node分散部署，相当于负载均衡的部署
     *
     * 未来可能引入AND,OR等逻辑组合
     */
    private String constraints;

    /**
     * 设置对container异常退出的重试次数
     * 有时，由于配置错误container可能永远也不会正确运行，这种情况下可以设置一个最多重试次数
     */
    private Integer retryThreshold;

    /**
     * 设置启动AM时的Java Options
     */
    private List<String> javaOptions;

    /**
     * 设置delegation token的renewer，默认是yarn，但有的时候需要改成跟yarn.resourcemanager.principal一样，
     * 否则rn向nn申请delegation token的时候会报错 xxx tries to renew a token (...) with non-matching renewer xxx
     */
    private String delegationTokenRenewer;

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getApplicationMasterJar() {
        return applicationMasterJar;
    }

    public void setApplicationMasterJar(String applicationMasterJar) {
        this.applicationMasterJar = applicationMasterJar;
    }
    public void setApplicationMasterClass(String applicationMasterClass){
        this.applicationMasterClass  = applicationMasterClass;
    }

    public String getApplicationMasterClass() {
        return applicationMasterClass;
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public void setHadoopConfDir(String hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
    }

    public YarnConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(YarnConfiguration configuration) {
        this.configuration = configuration;
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

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    public String getConstraints() {
        return constraints;
    }

    public void setConstraints(String constraints) {
        this.constraints = constraints;
    }

    public Integer getRetryThreshold() {
        return retryThreshold;
    }

    public void setRetryThreshold(Integer retryThreshold) {
        this.retryThreshold = retryThreshold;
    }

    public List<String> getJavaOptions() {
        return javaOptions;
    }

    public void setJavaOptions(List<String> javaOptions) {
        this.javaOptions = javaOptions;
    }

    public String getDelegationTokenRenewer() {
        return delegationTokenRenewer;
    }

    public void setDelegationTokenRenewer(String delegationTokenRenewer) {
        this.delegationTokenRenewer = delegationTokenRenewer;
    }

    private void checkIfNullOrEmpty(String t, String literal) throws InvalidClientArgumentException {
        if (t == null || t.isEmpty()) {
            throw new InvalidClientArgumentException("Invalid null or empty argument " + literal);
        }
    }

    public void checkSubmit() throws InvalidClientArgumentException {
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

    public void checkApp() throws InvalidClientArgumentException {
        checkIfNullOrEmpty(this.applicationId, "applicationId");
    }
}
