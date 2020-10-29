package com.eoi.marayarn.logstash;

public class XPackInfo {
    public Build build;
    public License license;

    public static class Build {
        public String hash;
        public String date;
    }

    public static class License {
        public String uid;
        public String type;
        public String mode;
        public String status;
        public Long expiry_date_in_millis;
    }

    public static XPackInfo buildMockXPackInfo() {
        XPackInfo xpackInfo = new XPackInfo();
        Build build = new Build();
        build.hash = "Unknown";
        build.date = "Unknown";
        License license = new License();
        license.uid = "a299836e-a51f-4ad8-8041-c3c653fe4012";
        license.type = "platinum";
        license.mode = "platinum";
        license.status = "active";
        license.expiry_date_in_millis = 3107746200000L;
        xpackInfo.build = build;
        xpackInfo.license = license;
        return xpackInfo;
    }
}
