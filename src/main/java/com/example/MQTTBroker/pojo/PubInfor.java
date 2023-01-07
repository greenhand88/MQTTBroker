package com.example.MQTTBroker.pojo;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import lombok.AllArgsConstructor;
import lombok.Data;
@Data
public class PubInfor {
    private MqttFixedHeader mqttFixedHeader;
    private MqttPublishVariableHeader mqttPublishVariableHeader;
    private ByteBuf byteBuf;
    private String topic;
    public PubInfor(ByteBuf byteBuf,String topic){
        this.byteBuf=byteBuf;
        this.topic=topic;
    }
    public PubInfor(MqttFixedHeader mqttFixedHeader,MqttPublishVariableHeader mqttPublishVariableHeader,ByteBuf byteBuf){
        this.mqttFixedHeader=mqttFixedHeader;
        this.mqttPublishVariableHeader=mqttPublishVariableHeader;
        this.byteBuf=byteBuf;
    }
}
