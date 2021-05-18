package com.eoi.marayarn;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

import java.util.Objects;

public class Artifact {
    private String localPath;
    private LocalResourceType type = LocalResourceType.FILE;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Artifact artifact = (Artifact) o;
        return Objects.equals(localPath, artifact.localPath) &&
                type == artifact.type &&
                visibility == artifact.visibility &&
                Objects.equals(hadoopConfDir, artifact.hadoopConfDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(localPath, type, visibility, hadoopConfDir);
    }
}