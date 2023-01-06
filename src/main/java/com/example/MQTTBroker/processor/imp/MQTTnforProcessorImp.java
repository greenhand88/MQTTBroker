package com.example.MQTTBroker.processor.imp;
import com.example.MQTTBroker.processor.MQTTInforProcessor;
import com.example.MQTTBroker.tool.NettyAutowireTool;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/*
为处理各种MQTT请求提供方法
 */
@Slf4j
public class MQTTnforProcessorImp implements MQTTInforProcessor {

    public RedisTemplate<String,Object>redisTemplate= (RedisTemplate<String,Object>)NettyAutowireTool.getBean("redisTemplate");
    private volatile static Map<String,Channel>map=new ConcurrentHashMap<>();
    private static ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(10, 100,
            10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(200));
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
            map.put(channel.id().asLongText(),channel);
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
            MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
            MqttFixedHeader mqttFixedHeader = mqttPublishMessage.fixedHeader();
            MqttQoS mqttQoS = (MqttQoS) mqttFixedHeader.qosLevel();//获取等级
            byte[] infor = new byte[mqttPublishMessage.payload().readableBytes()];//缓存
            mqttPublishMessage.payload().readBytes(infor);
            //业务代码
            String topic=mqttPublishMessage.variableHeader().topicName();
            //发布信息
            MqttMessage mqttMessage1=new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH,mqttFixedHeader.isDup(), MqttQoS.AT_MOST_ONCE, mqttFixedHeader.isRetain(), 0x02)
                    ,mqttPublishMessage.variableHeader()
                    , Unpooled.wrappedBuffer(infor));
            redisTemplate.opsForSet().members(topic)
                    .stream()
                    .map(object -> (String)object)
                    .forEach(channel1 -> threadPoolExecutor.execute(()-> {
                        if(map.containsKey(channel1))
                            map.get(channel1).writeAndFlush(mqttMessage1);
                        else
                            redisTemplate.opsForSet().remove(topic,channel1);//惰性去除Redis中无效的数据
                    }));
            //todo:进行一些业务处理
            if(mqttQoS.equals("AT_MOST_ONCE")){//至多一次
                return;
            }
            if(mqttQoS.equals("AT_LEAST_ONCE")){//至少一次
                MqttMessageIdVariableHeader backMqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId());
                MqttFixedHeader backMqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK,mqttFixedHeader.isDup(), MqttQoS.AT_MOST_ONCE, mqttFixedHeader.isRetain(), 0x02);
                MqttPubAckMessage pubAck = new MqttPubAckMessage(backMqttFixedHeader, backMqttMessageIdVariableHeader);
                log.trace("Response:{}",pubAck);
                channel.writeAndFlush(pubAck);
                return;
            }
            if(mqttQoS.equals("EXACTLY_ONCE")){//刚好一次
                MqttFixedHeader backMqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC,false, MqttQoS.AT_LEAST_ONCE,false,0x02);
                MqttMessageIdVariableHeader backMqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId());
                MqttMessage backMqttMessage = new MqttMessage(backMqttFixedHeader,backMqttMessageIdVariableHeader);
                log.trace("Response:{}",backMqttMessage);
                channel.writeAndFlush(backMqttMessage);
                return;
            }
        }catch (ClassCastException e){
            log.error("发布确认出现问题:{}",e);
            log.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",mqttMessage,"协议不支持");
        }catch (Exception e){
            log.error("发布确认出现问题:{}",e);
        }
    }

    @Override
    public void pubFin(Channel channel, MqttMessage mqttMessage) {
        try{
            MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
            MqttFixedHeader backmqttFixedHeader= new MqttFixedHeader(MqttMessageType.PUBCOMP,false, MqttQoS.AT_MOST_ONCE,false,0x02);
            MqttMessageIdVariableHeader backMqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
            MqttMessage backMqttMessage = new MqttMessage(backmqttFixedHeader,backMqttMessageIdVariableHeader);
            log.trace("Response:{}",backMqttMessage);
            channel.writeAndFlush(backMqttMessage);
        }catch (ClassCastException e){
            log.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",mqttMessage,"协议不支持");
        }catch (Exception e){
            log.error("Error:{}",e);
        }
    }

    @Override
    public void subAck(Channel channel, MqttMessage mqttMessage) {
        try{
            MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) mqttMessage;
            MqttMessageIdVariableHeader mqttMessageIdVariableHeader = mqttSubscribeMessage.variableHeader();
            //todo:进行业务处理
            Set<String> topics = mqttSubscribeMessage.payload()
                    .topicSubscriptions()
                    .stream()
                    .map(mqttTopicSubscription -> mqttTopicSubscription.topicName())
                    .collect(Collectors.toSet());
            List<Integer> grantedQoSLevels = new ArrayList<>(topics.size());
            for (int i = 0; i < topics.size(); i++) {
                grantedQoSLevels.add(mqttSubscribeMessage.payload()
                        .topicSubscriptions().get(i).qualityOfService().value());
            }
            topics.stream().forEach(topic ->redisTemplate.opsForSet().add(topic,channel.id().asLongText()));
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
