package com.eoi.marayarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class MiniYarnCluster implements Closeable {

    private final String testName;
    private final int nm;
    private MiniYARNCluster yarnCluster;
    private String hadoopConfDir;

    public MiniYarnCluster(String testName, int nm) {
        this.testName = testName;
        this.nm = nm;
    }

    public void start() throws Exception {
        File testDataPath = new File(PathUtils.getTestDir(ClientTest.class), "miniclusters");
        Configuration conf = new YarnConfiguration();
        //disable disk space health check
        conf.set(YarnConfiguration.NM_DISK_HEALTH_CHECK_ENABLE, "false");
        conf.set(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, "99");
        conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "1");
        yarnCluster = new MiniYARNCluster(testName, nm, 1, 1);
        yarnCluster.init(conf);
        yarnCluster.start();
        File xml = new File(testDataPath, "yarn-site.xml");
        xml.getParentFile().mkdirs();
        FileOutputStream fos = new FileOutputStream(xml);
        conf.writeXml(fos);
        fos.close();
        this.hadoopConfDir = xml.getParentFile().getAbsolutePath();
    }

    public Configuration getConfiguration() {
        return this.yarnCluster.getConfig();
    }

    /**
     * get after start successful
     * @return hadoopConfDir
     */
    public String getHadoopConfDir() {
        return this.hadoopConfDir;
    }

    @Override
    public void close() throws IOException {
        if (yarnCluster != null) {
            yarnCluster.stop();
        }
    }
}
