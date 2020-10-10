package com.eoi.marayarn;

import java.util.List;

public class AMArguments {
    private int numExecutors;
    private int executorCores;
    private int executorMemory; //MB
    private String queue;
    private String commandLine;
    private String principal;
    private String keytab;
    private String constraints;
    private List<ContainerLocation> location;

    public int getNumExecutors() {
        return numExecutors;
    }

    public AMArguments setNumExecutors(int numExecutors) {
        this.numExecutors = numExecutors;
        return this;
    }

    public int getExecutorCores() {
        return executorCores;
    }

    public AMArguments setExecutorCores(int executorCores) {
        this.executorCores = executorCores;
        return this;
    }

    public int getExecutorMemory() {
        return executorMemory;
    }

    public AMArguments setExecutorMemory(int executorMemory) {
        this.executorMemory = executorMemory;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public AMArguments setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public AMArguments setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getPrincipal() {
        return principal;
    }

    public AMArguments setPrincipal(String principal) {
        this.principal = principal;
        return this;
    }

    public String getKeytab() {
        return keytab;
    }

    public AMArguments setKeytab(String keytab) {
        this.keytab = keytab;
        return this;
    }

    public String getConstraints() {
        return constraints;
    }

    public AMArguments setConstraints(String constraints) {
        this.constraints = constraints;
        return this;
    }

    public List<ContainerLocation> getLocation() {
        return location;
    }

    public AMArguments setLocation(List<ContainerLocation> location) {
        this.location = location;
        return this;
    }
}
