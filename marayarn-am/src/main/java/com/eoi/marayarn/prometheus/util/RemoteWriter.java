package com.eoi.marayarn.prometheus.util;

import com.eoi.marayarn.prometheus.protobuf.Remote;
import com.eoi.marayarn.prometheus.protobuf.Types;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.net.URI;
import java.util.List;

public class RemoteWriter {

    private static final Logger logger = LoggerFactory.getLogger(RemoteWriter.class);
    private static final RequestConfig config =
            RequestConfig.custom().setSocketTimeout(5000).setConnectTimeout(5000).build();

    public static void writeTimeSeries(List<Types.TimeSeries> timeSeriesList, URI remoteEndpoint) throws Exception {
        if (timeSeriesList == null)
            return;
        Remote.WriteRequest writeRequest = new Remote.WriteRequest();
        writeRequest.setTimeseries(timeSeriesList);
        byte[] snappyBytes = Snappy.compress(writeRequest.toByteArray());
        HttpPost hp = new HttpPost(remoteEndpoint);
        hp.addHeader("Content-Type","application/x-protobuf");
        hp.addHeader("content-encoding","snappy");
        hp.setEntity(new ByteArrayEntity(snappyBytes, ContentType.DEFAULT_BINARY));
        try(CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(config).build()) {
            CloseableHttpResponse response = client.execute(hp);
            if (response.getStatusLine().getStatusCode() >= 300 || response.getStatusLine().getStatusCode() < 200) {
                logger.warn("Failed to write metrics to {}, status code {}",
                        remoteEndpoint, response.getStatusLine().getStatusCode());
            }
        }
    }
}
