package com.eoi.marayarn.http.model;

import com.eoi.marayarn.ApplicationMasterArguments;

import java.util.Optional;

public class ApplicationRequirement {
    public Integer numExecutors;
    public Integer executorCores;
    public Integer executorMemory; //MB
    public String queue;
    public String commandLine;

    public ApplicationMasterArguments mergeApplicationMasterArguments(ApplicationMasterArguments old) {
        ApplicationMasterArguments arguments = new ApplicationMasterArguments();
        arguments.numExecutors = Optional.ofNullable(numExecutors).orElse(old.numExecutors);
        arguments.executorCores = Optional.ofNullable(executorCores).orElse(old.executorCores);
        arguments.executorMemory = Optional.ofNullable(executorMemory).orElse(old.executorMemory);
        arguments.queue = Optional.ofNullable(queue).orElse(old.queue);
        arguments.commandLine = Optional.ofNullable(commandLine).orElse(old.commandLine);
        return arguments;
    }
}
