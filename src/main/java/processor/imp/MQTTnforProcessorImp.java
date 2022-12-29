package processor.imp;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import processor.MQTTInforProcessor;

/*
为处理各种MQTT请求提供方法
 */
public class MQTTnforProcessorImp implements MQTTInforProcessor{
    private Logger logger= LoggerFactory.getLogger(this.getClass());
    @Override
    public void conAck(Channel channel, MqttMessage mqttMessage) {
        //todo:加入身份验证
        try{
            MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) mqttMessage;
            MqttFixedHeader mqttFixedHeaderInfo = mqttConnectMessage.fixedHeader();
            MqttConnectVariableHeader mqttConnectVariableHeaderInfo = mqttConnectMessage.variableHeader();
            //返回报文可变报头
            MqttConnAckVariableHeader mqttConnAckVariableHeaderBack = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, mqttConnectVariableHeaderInfo.isCleanSession());
            //返回报文固定报头
            MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.CONNACK, mqttFixedHeaderInfo.isDup(), MqttQoS.AT_MOST_ONCE, true, 0x02);
            //构建CONNACK消息体
            MqttConnAckMessage connAck = new MqttConnAckMessage(mqttFixedHeaderBack, mqttConnAckVariableHeaderBack);
            logger.trace("Response:{}",connAck);
            channel.writeAndFlush(connAck);
        }catch (ClassCastException e){
            logger.error("请求转换格式失败!请求体格式:{}/n可能原因:{}",mqttMessage,"协议不支持");
        }catch (Exception e){
            logger.error("Error:{}",e);
        }
    }

    @Override
    public void pubAck(Channel channel, MqttMessage mqttMessage) {

    }

    @Override
    public void pubFin(Channel channel, MqttMessage mqttMessage) {

    }

    @Override
    public void subAck(Channel channel, MqttMessage mqttMessage) {

    }

    @Override
    public void unsubAck(Channel channel, MqttMessage mqttMessage) {

    }

    @Override
    public void heartBeatAck(Channel channel, MqttMessage mqttMessage) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage mqttMessageBack = new MqttMessage(fixedHeader);

        channel.writeAndFlush(mqttMessageBack);
    }
}
