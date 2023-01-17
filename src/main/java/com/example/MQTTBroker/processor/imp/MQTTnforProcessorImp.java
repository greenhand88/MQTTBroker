package com.example.MQTTBroker.processor.imp;
import com.example.MQTTBroker.handler.MQTTInforHandler;
import com.example.MQTTBroker.processor.MQTTInforProcessor;
import com.example.MQTTBroker.tool.NettyAutowireTool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/*
为处理各种MQTT请求提供方法
 */
@Slf4j
public class MQTTnforProcessorImp implements MQTTInforProcessor {
    private RedisTemplate<String,Object> redisTemplate= (RedisTemplate<String,Object>) NettyAutowireTool.getBean("redisTemplate");
    private Map<String,byte[]>latestInfor=new ConcurrentHashMap<>();//用于Qos1等级不同topic最新
    @Override
    public void conAck(Channel channel, MqttMessage mqttMessage) {
        //todo:加入身份验证
        try{
            MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) mqttMessage;
            MqttFixedHeader mqttFixedHeader = mqttConnectMessage.fixedHeader();
            MqttConnectVariableHeader mqttConnectVariableHeader = mqttConnectMessage.variableHeader();
            //返回报文可变报头
            MqttConnAckVariableHeader backmqttConnAckVariableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, mqttConnectVariableHeader.isCleanSession());
            //返回报文固定报头
            MqttFixedHeader backMqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, mqttFixedHeader.isDup(), MqttQoS.AT_MOST_ONCE, mqttFixedHeader.isRetain(), 0x02);
            //构建CONNACK消息体
            MqttConnAckMessage connAck = new MqttConnAckMessage(backMqttFixedHeader, backmqttConnAckVariableHeader);
            log.trace("Response:{}",connAck);
            channel.writeAndFlush(connAck);
        }catch (ClassCastException e){
            log.error("连接确认出现问题:{}",e);
            log.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",mqttMessage,"协议不支持");
        }catch (Exception e){
            log.error("连接确认出现问题:{}",e);
        }
    }

    @Override
    public void pubAck(Channel channel, MqttMessage mqttMessage) {
        try{
            Map<String, Channel>map= MQTTInforHandler.map;
            ThreadPoolExecutor threadPoolExecutor=MQTTInforHandler.threadPoolExecutor;
            MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
            byte[]data=new byte[mqttPublishMessage.payload().readableBytes()];
            mqttPublishMessage.payload().readBytes(data);
            MqttFixedHeader mqttFixedHeader = mqttPublishMessage.fixedHeader();
            MqttQoS mqttQoS = (MqttQoS) mqttFixedHeader.qosLevel();//获取等级
            //业务代码
            String topic=mqttPublishMessage.variableHeader().topicName();
            //发布信息
            //发布者和broker之间
            redisTemplate.opsForHash().scan(topic, ScanOptions.NONE)//给在线的订阅者直接获得信息
                    .stream()
                    .forEach(channel1 -> threadPoolExecutor.execute(()-> {
                        if(map.containsKey((String)channel1.getKey())) {
                            MqttQoS mqttQoS1=(MqttQoS)channel1.getValue();
                            ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
                            map.get((String)channel1.getKey()).
                                    writeAndFlush(new MqttPublishMessage(
                                            new MqttFixedHeader(MqttMessageType.PUBLISH, mqttFixedHeader.isDup(),mqttQoS1, mqttFixedHeader.isRetain(), mqttFixedHeader.remainingLength()),
                                            new MqttPublishVariableHeader(mqttPublishMessage.variableHeader().topicName(), mqttPublishMessage.variableHeader().packetId()),
                                            byteBuf
                                    ));
                        }
                        //else //多个服务会导致误删
                            //redisTemplate.opsForHash().delete(topic,channel1);//惰性去除Redis中无效的数据
                    }));

            if(mqttQoS.equals("AT_LEAST_ONCE")){//至少一次,
                MqttMessageIdVariableHeader backMqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId());
                MqttFixedHeader backMqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK,mqttFixedHeader.isDup(), MqttQoS.AT_MOST_ONCE, mqttFixedHeader.isRetain(), 0x02);
                MqttPubAckMessage pubAck = new MqttPubAckMessage(backMqttFixedHeader, backMqttMessageIdVariableHeader);
                log.trace("Response:{}",pubAck);
                channel.writeAndFlush(pubAck);
            }
        }catch (ClassCastException e){
            log.error("发布确认出现问题:{}",e);
            //log.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",mqttMessage,"协议不支持");
        }catch (Exception e){
            log.error("发布确认出现问题:{}",e);
        }
    }

    @Override
    public void subAck(Channel channel, MqttMessage mqttMessage) {
        try{
            ThreadPoolExecutor threadPoolExecutor=MQTTInforHandler.threadPoolExecutor;
            MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) mqttMessage;
            MqttMessageIdVariableHeader mqttMessageIdVariableHeader = mqttSubscribeMessage.variableHeader();
            //todo:进行业务处理
            Map<String, MqttQoS> topics = mqttSubscribeMessage.payload()
                    .topicSubscriptions()
                    .stream()
                    .collect(Collectors.toMap(
                            mqttTopicSubscription -> mqttTopicSubscription.topicName(),
                            mqttTopicSubscription -> mqttTopicSubscription.qualityOfService()
                    ));
            List<Integer> grantedQoSLevels = new ArrayList<>(topics.size());
            for (int i = 0; i < topics.size(); i++) {
                grantedQoSLevels.add(mqttSubscribeMessage.payload()
                        .topicSubscriptions().get(i).qualityOfService().value());
            }
            topics.keySet().stream().forEach(topic -> threadPoolExecutor.execute(()->redisTemplate.opsForHash().put(topic,channel.id().asLongText(),topics.get(topic))));
            /*
            topics.stream().forEach(topic-> threadPoolExecutor.execute(()->{
                                String welcome="Welcome to "+topic+"!";
                                ByteBuf byteBuf=Unpooled.wrappedBuffer(welcome.getBytes(CharsetUtil.UTF_8));
                                channel.writeAndFlush(new MqttPublishMessage(
                                        new MqttFixedHeader(MqttMessageType.PUBLISH,false,MqttQoS.AT_MOST_ONCE,false,0x02),
                                        new MqttPublishVariableHeader(topic,0),
                                        byteBuf
                                ));
                            })
                    );*/
            MqttMessageIdVariableHeader backMqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(mqttMessageIdVariableHeader.messageId());
            //构建返回报文,有效负载
            MqttSubAckPayload backMqttSubAckPayload= new MqttSubAckPayload(grantedQoSLevels);
            //构建返回报文,固定报头
            MqttFixedHeader backMqttFixedHeader= new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2+topics.size());
            //构建返回报文,订阅确认
            MqttSubAckMessage subAck = new MqttSubAckMessage(backMqttFixedHeader,backMqttMessageIdVariableHeader, backMqttSubAckPayload);
            log.trace("Response:{}",subAck);
            channel.writeAndFlush(subAck);
        }catch (ClassCastException e){
            log.error("订阅确认出现问题:{}",e);
            log.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",mqttMessage,"协议不支持");
        }catch (Exception e){
            log.error("订阅确认出现问题:{}",e);
        }
    }

    @Override
    public void unsubAck(Channel channel, MqttMessage mqttMessage) {
        try{
            ThreadPoolExecutor threadPoolExecutor=MQTTInforHandler.threadPoolExecutor;
            //业务代码
            MqttUnsubscribeMessage msg=(MqttUnsubscribeMessage)mqttMessage;
            msg.payload().topics().stream().forEach(topic -> threadPoolExecutor.execute(()->redisTemplate.opsForHash().delete(topic,channel.id().asLongText())));
            MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
            MqttMessageIdVariableHeader backMqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
            MqttFixedHeader backMqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttUnsubAckMessage unSubAck = new MqttUnsubAckMessage(backMqttFixedHeader,backMqttMessageIdVariableHeader);
            channel.writeAndFlush(unSubAck);
            log.trace("Response:{}",unSubAck);
        }catch (ClassCastException e){
            log.error("取消订阅确认出现问题:{}",e);
            log.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",mqttMessage,"协议不支持");
        }catch (Exception e){
            log.error("取消订阅确认出现问题:{}",e);
        }
    }

    @Override
    public void heartBeatAck(Channel channel, MqttMessage mqttMessage) {
        try{
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessage mqttMessageBack = new MqttMessage(fixedHeader);
            log.trace("Response:{}",mqttMessageBack);
            channel.writeAndFlush(mqttMessageBack);
        }catch (Exception e){
            log.error("心跳确认出现问题:{}",e);
        }
    }
}
