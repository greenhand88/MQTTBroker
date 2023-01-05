package com.example.MQTTBroker.bootstrap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.example.MQTTBroker.socketchannel.MySocketChannel;

@Slf4j
public class Server {
    private ServerBootstrap serverBootstrap;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private int EndPointNum=200;
    private int port=8888;

    public void startServer() {
        serverBootstrap=new ServerBootstrap();
        makeServer();
    }
    private void makeServer(){
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try{
            serverBootstrap.group(bossGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)//设置为nio
                    .option(ChannelOption.SO_BACKLOG, EndPointNum)
                    .option(ChannelOption.SO_REUSEADDR,true)//允许端口复用
                    .childOption(ChannelOption.SO_KEEPALIVE,true)
                    .childHandler(new MySocketChannel());
            log.info("com.example.MQTTBroker.bootstrap.Server Ready!");
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();//绑定端口号并启动服务端
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
            log.info("Shutdown Server successfully!");
        }
    }
}
