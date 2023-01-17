package com.example.MQTTBroker.processor;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;

public interface MQTTInforProcessor {

    /**
     * 连接确认
     * @param channel
     * @param mqttMessage
     */
    public void conAck (Channel channel, MqttMessage mqttMessage);

    /**
     * 发布确认
     * @param channel
     * @param mqttMessage
     */
    public void pubAck (Channel channel, MqttMessage mqttMessage);

    /**
     * 订阅确认
     * @param channel
     * @param mqttMessage
     */
    public void subAck(Channel channel, MqttMessage mqttMessage);

    /**
     * 取消订阅确认
     * @param channel
     * @param mqttMessage
     */
    public void unsubAck(Channel channel, MqttMessage mqttMessage);

    /**
     * 心跳确认
     * @param channel
     * @param mqttMessage
     */
    public void heartBeatAck (Channel channel, MqttMessage mqttMessage);
}
