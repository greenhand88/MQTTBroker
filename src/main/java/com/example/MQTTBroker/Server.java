package com.example.MQTTBroker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.example.MQTTBroker.socketchannel.MySocketChannel;

@Slf4j
@SpringBootApplication
public class Server {
    private static volatile ServerBootstrap serverBootstrap;
    @Value("${my.EndPointNum}")
    private int EndPointNum=128;
    @Value("${my.Port}")
    private int port=8080;
    public static void main(String[] args) {
        SpringApplication app=new SpringApplication(Server.class);
        app.run(args);
        new Server().startServer();
    }
    private void startServer() {
        if(serverBootstrap!=null){
            synchronized (serverBootstrap){
                if(serverBootstrap!=null){
                    serverBootstrap=new ServerBootstrap();
                }
            }
        }
        makeServer(serverBootstrap);
    }
    private void makeServer(ServerBootstrap bootstrap){
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try{
            bootstrap.group(bossGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)//设置为nio
                    .option(ChannelOption.SO_BACKLOG, EndPointNum)
                    .option(ChannelOption.SO_REUSEADDR,true)//允许端口复用
                    .childOption(ChannelOption.SO_KEEPALIVE,true)
                    .childHandler(new MySocketChannel());

            log.info("com.example.MQTTBroker.handler.Server Ready!");
            ChannelFuture channelFuture = bootstrap.bind(port).sync();//绑定端口号并启动服务端
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if(channelFuture.isSuccess())
                        log.info("端口监听启动正常!");
                    else
                        log.error("端口监听启动失败");
                }
            });
            channelFuture.channel().closeFuture().sync();//对关闭通道进行监听
        }catch (Exception e){
            log.error("Error:{}",e);
        }finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
