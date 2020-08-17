package com.eoi.marayarn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All arguments the Client accepted
 */
public class ClientArguments {
    // (Required) ApplicationMaster的jar包路径
    private String ApplicationMasterJar;
    // 默认会用com.eoi.marayarn.ApplicationMaster
    private String ApplicationMasterClass;
    // (Required) Application名字
    private String ApplicationName;
    // 资源池队列名
    private String Queue;
    // 设置Executor的vcpu
    private int Cpu;
    // 设置Executor的memory, 单位MB
    private int Memory;
    // 设置Executor的实例数量
    private int Instances;
    private String User;
    // Application Master的启动环境变量
    private Map<String, String> AMEnvironments;
    // Executor的启动环境变量
    private Map<String, String> ExecutorEnvironments;
    // Executor需要的物料，本地路径的集合，Client会上传这些到hdfs
    private List<Artifact> Artifacts;
    // (Required) 启动Executor的命令
    private String Command;

    public String getApplicationMasterJar() {
        return ApplicationMasterJar;
    }

    public void setApplicationMasterJar(String applicationMasterJar) {
        ApplicationMasterJar = applicationMasterJar;
    }

    public String getApplicationMasterClass() {
        return ApplicationMasterClass;
    }

    public void setApplicationMasterClass(String applicationMasterClass) {
        ApplicationMasterClass = applicationMasterClass;
    }

    public String getApplicationName() {
        return ApplicationName;
    }

    public void setApplicationName(String applicationName) {
        ApplicationName = applicationName;
    }

    public String getQueue() {
        return Queue;
    }

    public void setQueue(String queue) {
        Queue = queue;
    }

    public int getCpu() {
        return Cpu;
    }

    public void setCpu(int cpu) {
        Cpu = cpu;
    }

    public int getMemory() {
        return Memory;
    }

    public void setMemory(int memory) {
        Memory = memory;
    }

    public int getInstances() {
        return Instances;
    }

    public void setInstances(int instances) {
        Instances = instances;
    }

    public String getUser() {
        return User;
    }

    public void setUser(String user) {
        User = user;
    }

    public Map<String, String> getAMEnvironments() {
        return AMEnvironments;
    }

    public void setAMEnvironments(Map<String, String> AMEnvironments) {
        this.AMEnvironments = AMEnvironments;
    }

    public Map<String, String> getExecutorEnvironments() {
        return ExecutorEnvironments;
    }

    public void setExecutorEnvironments(Map<String, String> executorEnvironments) {
        ExecutorEnvironments = executorEnvironments;
    }

    public List<Artifact> getArtifacts() {
        return Artifacts;
    }

    public void setArtifacts(List<Artifact> artifacts) {
        Artifacts = artifacts;
    }

    public String getCommand() {
        return Command;
    }

    public void setCommand(String command) {
        Command = command;
    }


    private void checkIfNullOrEmpty(String t, String literal) throws InvalidClientArgumentException {
        if (t == null || t.isEmpty()) {
            throw new InvalidClientArgumentException("Invalid null or empty argument " + literal);
        }
    }

    public void check() throws InvalidClientArgumentException {
        if (this.ApplicationMasterClass == null || this.ApplicationMasterClass.isEmpty()) {
            this.ApplicationMasterClass = "com.eoi.marayarn.MaraApplicationMaster";
        }
        checkIfNullOrEmpty(this.ApplicationMasterJar, "ApplicationMasterJar");
        checkIfNullOrEmpty(this.ApplicationName, "ApplicationName");
        if (this.getCpu() <= 0) {
            this.setCpu(1);
        }
        if (this.getMemory() <= 0) {
            this.setMemory(512);
        }
        if (this.getInstances() <= 0) {
            this.setInstances(1);
        }
        checkIfNullOrEmpty(this.Command, "Command");
        if (this.AMEnvironments == null) {
            this.AMEnvironments = new HashMap<>();
        }
        if (this.ExecutorEnvironments == null) {
            this.ExecutorEnvironments = new HashMap<>();
        }
        if (this.Artifacts == null) {
            this.Artifacts = new ArrayList<>();
        }
    }
}
