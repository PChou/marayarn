package com.eoi.marayarn;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

public class Artifact {
    private String localPath;
    private LocalResourceType type;
    private LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;
    // since we need support file in another hdfs for localPath, so need to known the hadoopConfDir
    private String hadoopConfDir;

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

    public LocalResourceVisibility getVisibility() {
        return visibility;
    }

    public Artifact setVisibility(LocalResourceVisibility visibility) {
        this.visibility = visibility;
        return this;
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public Artifact setHadoopConfDir(String hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
        return this;
    }
}