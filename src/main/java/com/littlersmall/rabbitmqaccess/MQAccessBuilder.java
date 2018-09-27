package com.littlersmall.rabbitmqaccess;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;

import com.littlersmall.rabbitmqaccess.common.Constants;
import com.littlersmall.rabbitmqaccess.common.DetailRes;
import com.littlersmall.rabbitmqaccess.common.MessageWithTime;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by littlersmall on 16/5/11.
 */
@Slf4j
public class MQAccessBuilder {
    private ConnectionFactory connectionFactory;

    public MQAccessBuilder(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public MessageSender buildMessageSender(final String exchange, final String routingKey, final String queue) throws IOException {
        return buildMessageSender(exchange, routingKey, queue, "direct");
    }

    public MessageSender buildTopicMessageSender(final String exchange, final String routingKey) throws IOException {
        return buildMessageSender(exchange, routingKey, null, "topic");
    }

    //1 构造template, exchange, routingkey等
    //2 设置message序列化方法
    //3 设置发送确认
    //4 构造sender方法
    public MessageSender buildMessageSender(final String exchange, final String routingKey,
                                            final String queue, final String type) throws IOException {
        Connection connection = connectionFactory.createConnection();
        //1
        if (type.equals("direct")) {
            buildQueue(exchange, routingKey, queue, connection, "direct");
        } else if (type.equals("topic")) {
            buildTopic(exchange, connection);
        }

        final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);

        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setExchange(exchange);
        rabbitTemplate.setRoutingKey(routingKey);
        //2
        rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
        RetryCache retryCache = new RetryCache();

        //3
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.info("send message failed: " + cause + correlationData.toString());
            } else {
                retryCache.del(Long.valueOf(correlationData.getId()));
            }
        });

        rabbitTemplate.setReturnCallback((message, replyCode, replyText, tmpExchange, tmpRoutingKey) -> {
            try {
                Thread.sleep(Constants.ONE_SECOND);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            log.info("send message failed: " + replyCode + " " + replyText);
            rabbitTemplate.send(message);
        });

        //4
        return new MessageSender() {
            {
                retryCache.setSender(this);
            }

            @Override
            public DetailRes send(Object message) {
                long id = retryCache.generateId();
                long time = System.currentTimeMillis();

                return send(new MessageWithTime(id, time, message));
            }

            @Override
            public DetailRes send(MessageWithTime messageWithTime) {
                try {
                    retryCache.add(messageWithTime);
                    rabbitTemplate.correlationConvertAndSend(messageWithTime.getMessage(),
                            new CorrelationData(String.valueOf(messageWithTime.getId())));
                } catch (Exception e) {
                    return new DetailRes(false, "");
                }

                return new DetailRes(true, "");
            }
        };
    }

    public <T> MessageConsumer buildMessageConsumer(String exchange, String routingKey, final String queue,
                                                    final MessageProcess<T> messageProcess) throws IOException {
        return buildMessageConsumer(exchange, routingKey, queue, messageProcess, "direct");
    }

    public <T> MessageConsumer buildTopicMessageConsumer(String exchange, String routingKey, final String queue,
                                                         final MessageProcess<T> messageProcess) throws IOException {
        return buildMessageConsumer(exchange, routingKey, queue, messageProcess, "topic");
    }

    //1 创建连接和channel
    //2 设置message序列化方法
    //3 consume
    public <T> MessageConsumer buildMessageConsumer(String exchange, String routingKey, final String queue,
                                                     final MessageProcess<T> messageProcess, String type) throws IOException {
        final Connection connection = connectionFactory.createConnection();

        //1
        buildQueue(exchange, routingKey, queue, connection, type);

        //2
        final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();
        final MessageConverter messageConverter = new Jackson2JsonMessageConverter();

        //3
        return new MessageConsumer() {
            Channel channel;

            {
                channel = connection.createChannel(false);
            }

            //1 通过basicGet获取原始数据
            //2 将原始数据转换为特定类型的包
            //3 处理数据
            //4 手动发送ack确认
            @Override
            public DetailRes consume() {
                try {
                    //1
                    GetResponse response = channel.basicGet(queue, false);

                    while (response == null) {
                        response = channel.basicGet(queue, false);
                        Thread.sleep(Constants.ONE_SECOND);
                    }

                    Message message = new Message(response.getBody(),
                            messagePropertiesConverter.toMessageProperties(response.getProps(), response.getEnvelope(), "UTF-8"));

                    //2
                    @SuppressWarnings("unchecked")
                    T messageBean = (T) messageConverter.fromMessage(message);

                    //3
                    DetailRes detailRes;

                    try {
                        detailRes = messageProcess.process(messageBean);
                    } catch (Exception e) {
                        log.error("exception", e);
                        detailRes = new DetailRes(false, "process exception: " + e);
                    }

                    //4
                    if (detailRes.isSuccess()) {
                        channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    } else {
                        //避免过多失败log
                        Thread.sleep(Constants.ONE_SECOND);
                        log.info("process message failed: " + detailRes.getErrMsg());
                        channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
                    }

                    return detailRes;
                } catch (InterruptedException e) {
                    log.error("exception", e);
                    return new DetailRes(false, "interrupted exception " + e.toString());
                } catch (ShutdownSignalException | ConsumerCancelledException | IOException e) {
                    log.error("exception", e);

                    try {
                        channel.close();
                    } catch (IOException | TimeoutException ex) {
                        log.error("exception", ex);
                    }

                    channel = connection.createChannel(false);

                    return new DetailRes(false, "shutdown or cancelled exception " + e.toString());
                } catch (Exception e) {
                    log.info("exception : ", e);

                    try {
                        channel.close();
                    } catch (IOException | TimeoutException ex) {
                        ex.printStackTrace();
                    }

                    channel = connection.createChannel(false);

                    return new DetailRes(false, "exception " + e.toString());
                }
            }
        };
    }

    private void buildQueue(String exchange, String routingKey,
                            final String queue, Connection connection, String type) throws IOException {
        Channel channel = connection.createChannel(false);

        if (type.equals("direct")) {
            channel.exchangeDeclare(exchange, "direct", true, false, null);
        } else if (type.equals("topic")) {
            channel.exchangeDeclare(exchange, "topic", true, false, null);
        }

        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, routingKey);

        try {
            channel.close();
        } catch (TimeoutException e) {
            log.info("close channel time out ", e);
        }
    }

    private void buildTopic(String exchange, Connection connection) throws IOException {
        Channel channel = connection.createChannel(false);
        channel.exchangeDeclare(exchange, "topic", true, false, null);
    }

    //for test
    public int getMessageCount(final String queue) throws IOException {
        Connection connection = connectionFactory.createConnection();
        final Channel channel = connection.createChannel(false);
        final AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queue);

        return declareOk.getMessageCount();
    }
}
