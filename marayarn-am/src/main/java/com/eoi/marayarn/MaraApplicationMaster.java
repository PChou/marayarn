package com.eoi.marayarn;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;

import static com.eoi.marayarn.Constants.AM_ENV_COMMANDLINE;

public class MaraApplicationMaster {
    private static Logger logger = LoggerFactory.getLogger(MaraApplicationMaster.class);
    private NioServerSocketChannel channel;
    public YarnConfiguration configuration;
    public ApplicationAttemptId applicationAttemptId;
    public ApplicationMasterArguments arguments;
    public String trackingUrl;
    public String amContainerLogUrl;
    public String webSchema;
    public String user;
    @SuppressWarnings("rawtypes")
    public AMRMClient amClient;
    public YarnAllocator allocator;
    private volatile boolean finished;

    public void terminate() throws Exception {
        this.finished = true;
        unregister(FinalApplicationStatus.KILLED);
    }

    public String getAMContainerLogs() {
        return amContainerLogUrl;
    }

    private void initializePipeline(SocketChannel channel) {
        if ( channel == null )
            return;
        EventExecutorGroup eventHandlerGroup = new DefaultEventExecutorGroup(5);
        channel.pipeline()
                .addLast("http-decoder", new HttpRequestDecoder()) // 请求消息解码器
                .addLast("http-aggregator", new HttpObjectAggregator(512*1024)) // 目的是将多个消息转换为单一的request或者response对象
                .addLast("http-encoder", new HttpResponseEncoder())//响应解码器
                .addLast("http-chunked",new ChunkedWriteHandler())//目的是支持异步大文件传输
                .addLast(eventHandlerGroup, new HttpRequestHandler(this));
    }

    public void startHttpServer() throws Exception {
        ServerBootstrap bootstrap = new ServerBootstrap();
        NioEventLoopGroup group = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        bootstrap.group(group, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        initializePipeline(ch);
                    }
                });
        ChannelFuture f = bootstrap.bind(0).sync();
        if (f.isSuccess()) {
            channel = (NioServerSocketChannel) f.channel();
            logger.info("bind and listen at {}", channel.localAddress());
            channel.closeFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("Shutdown event loop");
                    group.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                }
            });
        } else {
            throw new RuntimeException("Failed to bind listener.");
        }
    }

    public void stopHttpServer() {
        logger.info("Stopping http server");
        if (channel != null) {
            try {
                channel.close().await();
            } catch (InterruptedException ex) {}
        }
    }

    public void initializeAM(ApplicationMasterArguments arguments) {
        // get Application Identify
        this.configuration = new YarnConfiguration();
        final String containerId = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
        this.applicationAttemptId = ConverterUtils.toContainerId(containerId).getApplicationAttemptId();
        logger.info("ApplicationAttemptId: {}, ApplicationId: {}", this.applicationAttemptId,
                this.applicationAttemptId.getApplicationId());
        final String node = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
        final String port = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
        final String user = System.getenv(ApplicationConstants.Environment.USER.name());
        final String schema = YarnConfiguration.useHttps(configuration) ? "https" : "http";
        this.webSchema = schema;
        this.user = user;
        this.amContainerLogUrl = String.format("%s://%s:%s/node/containerlogs/%s/%s", schema, node, port, containerId, user);
        this.amClient = AMRMClient.createAMRMClient();
        this.amClient.init(this.configuration);
        this.amClient.start();
        this.allocator = new YarnAllocator(this.configuration, this.amClient, this.applicationAttemptId, arguments);
    }

    public void register() throws YarnException, IOException {
        assert(channel != null);
        String hn = InetAddress.getLocalHost().getHostName();
        String hostname;
        if (hn != null) {
            hostname = hn;
        } else {
            hostname = InetAddress.getLocalHost().getHostAddress();
        }
        int port = channel.localAddress().getPort();
        trackingUrl = "http://" + hostname + ":" + port;
        logger.info("Register application with trackingUrl: {}", trackingUrl);
        this.amClient.registerApplicationMaster(hostname, port, trackingUrl);
    }

    public void unregister(FinalApplicationStatus status) throws YarnException, IOException {
        this.amClient.unregisterApplicationMaster(status, "", trackingUrl);
    }

    public static Options setupOptions() {
        Options options = new Options();
        Option instances = new com.eoi.marayarn.OptionBuilder("executors").hasArg(true).argName("int")
                .desc("The number of instance of executors").build();
        Option vcpu = new com.eoi.marayarn.OptionBuilder("cores").hasArg(true).argName("int")
                .desc("The number of vcores of every executor").build();
        Option memory = new com.eoi.marayarn.OptionBuilder("memory").hasArg(true).argName("int")
                .desc("Memory of every executor in MB").build();
        Option queue = new com.eoi.marayarn.OptionBuilder("queue").hasArg(true).argName("queueName")
                .desc("queueName").build();
        options.addOption(instances);
        options.addOption(vcpu);
        options.addOption(memory);
        options.addOption(queue);
        return options;
    }

    public static ApplicationMasterArguments toAMArguments(CommandLine commandLine) {
        ApplicationMasterArguments arguments = new ApplicationMasterArguments();
        arguments.numExecutors = getIntOrDefault(commandLine, "executors", 0);
        arguments.executorCores = getIntOrDefault(commandLine, "cores", 0);
        arguments.executorMemory = getIntOrDefault(commandLine, "memory", 0);
        arguments.queue = commandLine.getOptionValue("queue");
        arguments.commandLine = System.getenv(AM_ENV_COMMANDLINE);
        return arguments;
    }

    public static int getIntOrDefault(CommandLine argLine, String key, int defaultValue) {
        String v = argLine.getOptionValue(key);
        if (v == null || v.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(v);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Application Master");
        Options options = setupOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine argLine = parser.parse(options, args);
        MaraApplicationMaster applicationMaster = new MaraApplicationMaster();
        ApplicationMasterArguments arguments = toAMArguments(argLine);
        arguments.checkArguments();
        applicationMaster.arguments = arguments;
        logger.info("Checked arguments of Application Master {}", arguments);
        logger.info("Starting http server");
        applicationMaster.startHttpServer();
        Runtime.getRuntime().addShutdownHook(new Thread(applicationMaster::stopHttpServer));
        logger.info("Initializing and registering Application Master");
        applicationMaster.initializeAM(arguments);
        applicationMaster.register();
        logger.info("Begin to allocate resources");
        applicationMaster.allocator.allocateResources();
        logger.info("Starting reporter thread");
        int expiryInterval = applicationMaster.configuration.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000);
        int interval = Math.max(0, Math.min(expiryInterval / 2, 5000));
        while(!applicationMaster.finished) {
            try {
                applicationMaster.allocator.allocateResources();
            } catch (Exception ex) {
                logger.error("Reporter error ", ex);
            } finally {
                try {
                    Thread.sleep(interval);
                } catch (Exception ex) {
                    applicationMaster.finished = true;
                }
            }
        }
        System.exit(0);
    }
}
