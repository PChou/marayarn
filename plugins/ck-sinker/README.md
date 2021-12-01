# plugin-ck-sinker

该插件实现对[clickhouse_sinker](http://github.com/housepower/clickhouse_sinker)的监控和调度。监控指标写入prometheus

提交命令示例:

```
HADOOP_CONF_DIR=$PWD/hadoop marayarn_pkg/marayarn submit \
-am file:///$PWD/marayarn-am/target/marayarn-am-1.0.1-jar-with-dependencies.jar \
-cpu 1 -memory 2048 -name ck_test -instance 1 \
--file "file:///$PWD/marayarn_pkg/clickhouse_sinker" \
--file "file:///$PWD/marayarn_pkg/sensor_dt_result_online.json" \
--file "file://$PWD/plugins/ck-sinker/target/plugin-ck-sinker-1.0.1-jar-with-dependencies.jar" \
-cmd './clickhouse_sinker --local-cfg-file sensor_dt_result_online.json' \
-e 'PROMETHEUS_PUSHGATEWAY_ENDPOINT=192.168.21.41:9091' \
```

与一般的应用提交有如下区别:

1. 增加`plugin-ck-sinker-1.0.1-jar-with-dependencies.jar`的jar包上传
2. 为AM增加`PROMETHEUS_PUSHGATEWAY_ENDPOINT=192.168.21.41:9091`环境变量

`PROMETHEUS_PUSHGATEWAY_ENDPOINT`指向的是prometheus push gateway。

`plugin-ck-sinker`在启动`clickhouse_sinker`的时候会在环境变量中增加

```
METRICS_PUSH_GATEWAY_ADDR=http://node3:33445/cks/prom/write
```
- `METRICS_PUSH_GATEWAY_ADDR=http://node3:33445/cks/prom/write`是`plugin-ck-sinker`启动的http处理接口，用于接收`clickhouse_sinker`上报的指标数据.
`clickhouse_sinker`以普通的HTTP POST，将`prometheus`的标准格式的指标以标准文本的方式发送给`plugin-ck-sinker`即可

