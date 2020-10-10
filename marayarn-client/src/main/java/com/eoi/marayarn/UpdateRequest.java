package com.eoi.marayarn;

public class UpdateRequest {
    private int numExecutors;
    private int executorCores;
    private int executorMemory; //MB
    private String commandLine;
    private String constraints;

    public int getNumExecutors() {
        return numExecutors;
    }

    public UpdateRequest setNumExecutors(int numExecutors) {
        this.numExecutors = numExecutors;
        return this;
    }

    public int getExecutorCores() {
        return executorCores;
    }

    public UpdateRequest setExecutorCores(int executorCores) {
        this.executorCores = executorCores;
        return this;
    }

    public int getExecutorMemory() {
        return executorMemory;
    }

    public UpdateRequest setExecutorMemory(int executorMemory) {
        this.executorMemory = executorMemory;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public UpdateRequest setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getConstraints() {
        return constraints;
    }

    public UpdateRequest setConstraints(String constraints) {
        this.constraints = constraints;
        return this;
    }
}
