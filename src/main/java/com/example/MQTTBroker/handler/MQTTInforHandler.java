package com.example.MQTTBroker.handler;

import com.example.MQTTBroker.processor.MQTTInforProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import com.example.MQTTBroker.processor.imp.MQTTnforProcessorImp;

import java.util.Map;
import java.util.concurrent.*;

@Slf4j
@ChannelHandler.Sharable
public class MQTTInforHandler extends ChannelInboundHandlerAdapter {
    private static MQTTInforProcessor mqttInforProcessor=new MQTTnforProcessorImp();
    public volatile static Map<String,Channel> map=new ConcurrentHashMap<>();
    public static ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(10, 100,
            10
            , TimeUnit.SECONDS
            , new ArrayBlockingQueue<>(200)
            , new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    }
            ,
            new ThreadPoolExecutor.DiscardPolicy()
    );
    @Override
    public void channelRead(ChannelHandlerContext ctx,Object msg){
        if(msg==null){
            log.trace("请求为空!");
            return;
        }
        try{
            MqttMessage mqttMessage=(MqttMessage) msg;
            Channel channel=ctx.channel();
            map.put(channel.id().asLongText(),channel);
            inforHandler(channel,mqttMessage);
        }catch(ClassCastException e){
            log.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",msg,"协议不支持");
        }
    }
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        map.remove(ctx.channel().id().asLongText());
        ctx.fireChannelInactive();
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }

    private void inforHandler(Channel channel,MqttMessage mqttMessage){//对于各种情况的处理
        if(mqttMessage.fixedHeader().messageType().equals(MqttMessageType.CONNECT)){//建立连接的处理
            mqttInforProcessor.conAck(channel,mqttMessage);
            return;
        }
        if(mqttMessage.fixedHeader().messageType().equals(MqttMessageType.PINGREQ)){//心跳
            mqttInforProcessor.heartBeatAck(channel,mqttMessage);
            return;
        }
        if(mqttMessage.fixedHeader().messageType().equals(MqttMessageType.PUBLISH)){
            mqttInforProcessor.pubAck(channel,mqttMessage);
            return;
        }
        if(mqttMessage.fixedHeader().messageType().equals(MqttMessageType.PUBCOMP)){
            mqttInforProcessor.pubFin(channel,mqttMessage);
            return;
        }
        if(mqttMessage.fixedHeader().messageType().equals(MqttMessageType.SUBSCRIBE)){
            mqttInforProcessor.subAck(channel,mqttMessage);
            return;
        }
        if(mqttMessage.fixedHeader().messageType().equals(MqttMessageType.UNSUBSCRIBE)){
            mqttInforProcessor.unsubAck(channel,mqttMessage);
            return;
        }
    }
}
