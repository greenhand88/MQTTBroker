package com.example.MQTTBroker.socketchannel;

import com.example.MQTTBroker.handler.MQTTInforHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;

public class MySocketChannel extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        socketChannel.pipeline().addLast(new MqttDecoder());//MQTT解码器
        socketChannel.pipeline().addLast("encoder", MqttEncoder.INSTANCE);//编码器
        socketChannel.pipeline().addLast("decoder",new MQTTInforHandler());//解码器
    }
}
