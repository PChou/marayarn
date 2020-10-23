package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.clickhouse.sinker.parse.text.TextPrometheusMetricDataParser;
import com.eoi.marayarn.clickhouse.sinker.parse.types.*;
import com.eoi.marayarn.clickhouse.sinker.protobuf.Remote;
import com.eoi.marayarn.clickhouse.sinker.protobuf.Types;
import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerErrorException;
import com.eoi.marayarn.http.ProcessResult;
import com.eoi.marayarn.http.model.AckResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
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

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.*;

public class PromWriteHandler implements Handler {
    private MaraApplicationMaster applicationMaster;
    private static Logger logger = LoggerFactory.getLogger(PromWriteHandler.class);
    private static final RequestConfig config =
            RequestConfig.custom().setSocketTimeout(5000).setConnectTimeout(5000).build();

    public PromWriteHandler(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public Map<String, String> match(String uri, HttpMethod method) {
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        if (!uri.startsWith("/cks/prom/write") || !method.equals(HttpMethod.PUT) ) {
            return null;
        }
        return new HashMap<>();
    }

    @Override
    public ProcessResult process(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException {
        URL influxdbRemoteWriteURL = null;
        try {
            influxdbRemoteWriteURL = new URL(System.getenv(CKSinkerAMPlugin.INFLUXDB_URL_ENV_KEY));
        } catch (java.net.MalformedURLException e) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }

        try {
            long timestamp = System.currentTimeMillis();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body);
            // parse
            TextPrometheusMetricDataParser dataParser = new TextPrometheusMetricDataParser(byteArrayInputStream);
            MetricFamily metricFamily = dataParser.parse();
            List<Types.TimeSeries> tsList = new ArrayList<>(64);
            while (metricFamily != null) {
                for (Metric metric: metricFamily.getMetrics()) {
                    PromConverter.convertAndFill(tsList, metric, timestamp, this.applicationMaster.applicationAttemptId.getApplicationId().toString());
                }
                metricFamily = dataParser.parse();
            }
            Remote.WriteRequest writeRequest = new Remote.WriteRequest();
            writeRequest.setTimeseries(tsList);
            byte[] snappyBytes = Snappy.compress(writeRequest.toByteArray());
            HttpPost hp = new HttpPost(influxdbRemoteWriteURL.toURI());
            hp.addHeader("Content-Type","application/x-protobuf");
            hp.addHeader("content-encoding","snappy");
            hp.setEntity(new ByteArrayEntity(snappyBytes, ContentType.DEFAULT_BINARY));
            try(CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(config).build()) {
                CloseableHttpResponse response = client.execute(hp);
                if (response.getStatusLine().getStatusCode() >= 300 || response.getStatusLine().getStatusCode() < 200) {
                    logger.warn("Failed to write metrics to {}, status code {}",
                            influxdbRemoteWriteURL, response.getStatusLine().getStatusCode());
                }
            }
            return ProcessResult.jsonProcessResult(AckResponse.OK);
        } catch (Exception ex) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        }
    }
}
