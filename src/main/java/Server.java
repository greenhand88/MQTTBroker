
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import socketchannel.MySocketChannel;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class Server {

    public static void main(String[] args) {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try{
            HashMap <String,Object>hashMap = readConfig("");
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(bossGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)//设置为nio
                    .option(ChannelOption.SO_BACKLOG, (Integer) hashMap.get("EndPointNum"))
                    .option(ChannelOption.SO_REUSEADDR,true)//允许端口复用
                    //.childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE,true)
                    .childHandler(new MySocketChannel());

            System.out.println("Server Ready!");
            ChannelFuture channelFuture = bootstrap.bind(8888).sync();//绑定端口号并启动服务端
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if(channelFuture.isSuccess())
                        System.out.println("端口监听启动正常!");
                    else
                        System.out.println("端口监听启动失败");
                }
            });
            channelFuture.channel().closeFuture().sync();//对关闭通道进行监听
        }catch (IOException e){
            System.out.println("配置文件IO出错");
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
    private static HashMap readConfig(String filePath)throws IOException{
        if(filePath.length()==0){
            filePath= "Config.yml";
        }
        filePath="/"+filePath;
        ObjectMapper objectMapper=new ObjectMapper(new YAMLFactory());
        HashMap hashMap = objectMapper.readValue(new File(Server.class.getResource(filePath).getFile()), HashMap.class);
        return hashMap;
    }

}
