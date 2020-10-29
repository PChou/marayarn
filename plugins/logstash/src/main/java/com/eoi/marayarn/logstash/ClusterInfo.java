package com.eoi.marayarn.logstash;

public class ClusterInfo {
    public String name;
    public String cluster_name;
    public String cluster_uuid;
    public String tagline;
    public Version version;

    public static class Version {
        public String number;
        public String build_flavor;
        public String build_type;
        public String build_hash;
        public String build_date;
        public Boolean build_snapshot;
        public String lucene_version;
        public String minimum_wire_compatibility_version;
        public String minimum_index_compatibility_version;
    }

    public static ClusterInfo buildMockClusterInfo() {
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.name = "node-1";
        clusterInfo.cluster_name = "marayarn-mock-cluster";
        clusterInfo.cluster_uuid = "Y74Ymk-VSQS443D3BXs7Ug";
        clusterInfo.tagline = "You Know, for Search";
        Version version = new Version();
        version.number = "6.8.0"; // generate es 6 version
        version.build_flavor = "default";
        version.build_type = "tar";
        version.build_hash = "1fd8f69";
        version.build_snapshot = false;
        version.build_date = "2019-02-13T17:10:04.160291Z";
        version.lucene_version = "7.6.0";
        version.minimum_index_compatibility_version = "5.0.0";
        version.minimum_wire_compatibility_version = "5.6.0";
        clusterInfo.version = version;
        return clusterInfo;
    }
}
