package com.eoi.marayarn;

import org.apache.hadoop.yarn.api.records.LocalResourceType;

public class Artifact {
    private String localPath;
    private LocalResourceType type;

    public String getLocalPath() {
        return localPath;
    }

    public Artifact setLocalPath(String localPath) {
        this.localPath = localPath;
        return this;
    }

    public LocalResourceType getType() {
        return type;
    }

    public Artifact setType(LocalResourceType type) {
        this.type = type;
        return this;
    }
}