# marayarn

`marayarn`是`marathon-on-yarn`的简称，目标是在yarn资源管理框架下实现类似[marathon](https://github.com/mesosphere/marathon)的功能。

`marathon`是一款基于[Apache mesos](https://mesos.apache.org/)的，支持RESTful风格的资源调度平台。

`yarn`是hadoop2.0引入的资源管理框架，在yarn上一般运行`MapReduce`,`Spark`,`Flink`等计算框架。至今还没有一款类似marathon的调度平台，这就是本项目的意义。

项目目前仍然在开发阶段，已经实现的核心的功能包括：

1. 一个client sdk。支持提交一个命令到yarn上运行；支持指定实例数、每个实例的vcore和memory；支持设置资源文件(jar, tar.gz, zip, 配置文件等，资源文件支持hdfs://, file://, http://, ftp://形式的URI)

2. 一个负责管理当前提交应用的ApplicationMaster。支持通过RESTful接口进行状态查询、扩缩容、变更配置、停止。如果要更新资源文件，需要停止后，再通过client sdk提交新任务
3. 一个cli命令行工具，通过命令行提交应用



## 运行架构

![](doc/runtime.png)

## Roadmap

- [ ] yarn-queue支持
- [ ] 提交之前评估资源是否够用
- [ ] client sdk增强，client sdk支持状态查询、扩缩容、变更配置、停止
- [ ] cli对应支持增强后的client sdk
- [ ] 设计和实现一个类似marathon的管理界面，提供更高层次的RESTful接口，方便管理应用
- [ ] 文档和注释增强
