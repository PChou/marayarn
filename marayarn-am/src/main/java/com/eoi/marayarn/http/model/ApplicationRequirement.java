package com.eoi.marayarn.http.model;

import com.eoi.marayarn.ApplicationMasterArguments;

import java.util.Optional;

public class ApplicationRequirement {
    public Integer numExecutors;
    public Integer executorCores;
    public Integer executorMemory; //MB
    public String commandLine;
    public String constraints;

    public ApplicationMasterArguments mergeApplicationMasterArguments(ApplicationMasterArguments old) {
        ApplicationMasterArguments arguments = new ApplicationMasterArguments();
        arguments.numExecutors = Optional.ofNullable(numExecutors).orElse(old.numExecutors);
        arguments.executorCores = Optional.ofNullable(executorCores).orElse(old.executorCores);
        arguments.executorMemory = Optional.ofNullable(executorMemory).orElse(old.executorMemory);
        arguments.commandLine = Optional.ofNullable(commandLine).orElse(old.commandLine);
        arguments.constraints = Optional.ofNullable(constraints).orElse(old.constraints);
        return arguments;
    }
}
