package com.example.MQTTBroker.handler;

import com.example.MQTTBroker.pojo.PubInfor;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class PublishHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)  {
        ctx.write(msg, promise);
        if(msg instanceof PubInfor) {
            MqttFixedHeader mqttFixedHeader=new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
            MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(((PubInfor) msg).getTopic(), 0);
            if(((PubInfor) msg).getMqttFixedHeader()!=null){
                mqttFixedHeader=((PubInfor) msg).getMqttFixedHeader();
            }
            if(((PubInfor) msg).getMqttFixedHeader()!=null){
                mqttPublishVariableHeader=((PubInfor) msg).getMqttPublishVariableHeader();
            }
            MqttPublishMessage publishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, ((PubInfor) msg).getByteBuf());
            msg=publishMessage;
            ctx.writeAndFlush(msg);
            promise.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    if (future.isSuccess()) {
                        publishMessage.payload().release();
                    }
                }
            });
        }


    }
}
