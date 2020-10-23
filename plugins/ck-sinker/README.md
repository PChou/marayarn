# plugin-ck-sinker

该插件实现对[clickhouse_sinker](github.com/housepower/clickhouse_sinker)的监控和调度。监控指标写入influxdb

提交命令示例:

```
HADOOP_CONF_DIR=$PWD/hadoop marayarn/marayarn submit \
-am file:///$PWD/marayarn-am/target/marayarn-am-1.0-SNAPSHOT-jar-with-dependencies.jar \
-cpu 1 -memory 1024 -name ck_sinker_test -instance 2 \
--file "file:///Users/pchou/Projects/java/marayarn/marayarn/clickhouse_sinker.tar.gz#sinker" \
--file "file:///Users/pchou/Projects/java/marayarn/plugins/ck-sinker/target/plugin-ck-sinker-1.0-SNAPSHOT.jar" 
-cmd 'sinker/clickhouse_sinker -conf sinker/conf' \
-e 'INFLUXDB_URL=http://xxx.xxx.xxx.xxx:8086/api/v1/prom/write?db=cks'
```

与一般的应用提交有如下区别:

1. 增加`plugin-ck-sinker-1.0-SNAPSHOT.jar`的jar包上传
2. 为AM增加`INFLUXDB_URL=http://xxx.xxx.xxx.xxx:8086/api/v1/prom/write?db=cks`环境变量

`INFLUXDB_URL`指向的是influxdb关于支持`prometheus remote write`功能的接口

> 参见
> [Prometheus endpoints support in InfluxDB](https://docs.influxdata.com/influxdb/v1.8/supported_protocols/prometheus/)
> [remote write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write)
> [REMOTE WRITE TUNING](https://prometheus.io/docs/practices/remote_write/#remote-write-characteristics)

也就是`plugin-ck-sinker`将数据写入influxdb的方式是采用了跟`prometheus`一致。

`plugin-ck-sinker`在启动`clickhouse_sinker`的时候会在环境变量中增加

```
METRICS_PUSH_GATEWAY_ADDR=http://node3:33445/cks/prom/write
```
- `METRICS_PUSH_GATEWAY_ADDR=http://node3:33445/cks/prom/write`是`plugin-ck-sinker`启动的http处理接口，用于接收`clickhouse_sinker`上报的指标数据.
`clickhouse_sinker`以普通的HTTP POST，将`prometheus`的标准格式的指标以标准文本的方式发送给`plugin-ck-sinker`即可

