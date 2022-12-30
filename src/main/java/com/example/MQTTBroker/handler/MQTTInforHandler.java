package com.example.MQTTBroker.handler;

import com.example.MQTTBroker.processor.MQTTInforProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.MQTTBroker.processor.imp.MQTTnforProcessorImp;
@Slf4j
public class MQTTInforHandler extends ChannelInboundHandlerAdapter {
    private Logger logger =  LoggerFactory.getLogger(this.getClass());
    private static MQTTInforProcessor mqttInforProcessor=new MQTTnforProcessorImp();
    @Override
    public void channelRead(ChannelHandlerContext ctx,Object msg){
        if(msg==null){
            log.trace("请求为空!");
            return;
        }
        try{
            MqttMessage mqttMessage=(MqttMessage) msg;
            Channel channel=ctx.channel();
            inforHandler(channel,mqttMessage);
        }catch(ClassCastException e){
            log.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",msg,"协议不支持");
        }
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
