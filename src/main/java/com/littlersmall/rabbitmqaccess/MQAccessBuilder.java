package com.littlersmall.rabbitmqaccess;

import com.littlersmall.rabbitmqaccess.common.DetailRes;
import com.rabbitmq.client.*;
import lombok.extern.java.Log;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by littlersmall on 16/5/11.
 */
@Log
public class MQAccessBuilder {
    private ConnectionFactory connectionFactory;

    public MQAccessBuilder(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    //1 构造template, exchange, routingkey等
    //2 设置message序列化方法
    //3 设置发送确认
    //4 构造sender方法
    public MessageSender buildMessageSender(final String exchange, final String routingKey, final String queue) throws IOException, TimeoutException {
        Connection connection = connectionFactory.createConnection();
        //1
        buildQueue(exchange, routingKey, queue, connection);

        final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);

        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setExchange(exchange);
        rabbitTemplate.setRoutingKey(routingKey);
        //2
        rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());

        //3
        rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
            @Override
            public void confirm(CorrelationData correlationData, boolean ack, String cause) {
               if (!ack) {
                   log.info("send message failed: " + cause); //+ correlationData.toString());
                   throw new RuntimeException("send error " + cause);
               }
            }
        });

        //4
        return new MessageSender() {
            @Override
            public DetailRes send(Object message) {
                try {
                    rabbitTemplate.convertAndSend(message);
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    log.info("send failed " + e);

                    try {
                        //retry
                        rabbitTemplate.convertAndSend(message);
                    } catch (RuntimeException error) {
                        error.printStackTrace();
                        log.info("send failed again " + error);

                        return new DetailRes(false, error.toString());
                    }
                }

                return new DetailRes(true, "");
            }
        };
    }

    //1 创建连接和channel
    //2 设置message序列化方法
    //3 构造consumer
    public <T> MessageConsumer buildMessageConsumer(String exchange, String routingKey,
                                                    final String queue, final MessageProcess<T> messageProcess) throws IOException {
        final Connection connection = connectionFactory.createConnection();

        //1
        buildQueue(exchange, routingKey, queue, connection);

        //2
        final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();
        final MessageConverter messageConverter = new Jackson2JsonMessageConverter();

        //3
        return new MessageConsumer() {
            QueueingConsumer consumer;

            {
                consumer = buildQueueConsumer(connection, queue);
            }

            @Override
            //1 通过delivery获取原始数据
            //2 将原始数据转换为特定类型的包
            //3 处理数据
            //4 手动发送ack确认
            public DetailRes consume() {
                QueueingConsumer.Delivery delivery = null;
                Channel channel = consumer.getChannel();

                try {
                    //1
                    delivery = consumer.nextDelivery();
                    Message message = new Message(delivery.getBody(),
                            messagePropertiesConverter.toMessageProperties(delivery.getProperties(), delivery.getEnvelope(), "UTF-8"));

                    //2
                    @SuppressWarnings("unchecked")
                    T messageBean = (T) messageConverter.fromMessage(message);

                    //3
                    DetailRes detailRes = messageProcess.process(messageBean);

                    //4
                    if (detailRes.isSuccess()) {
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } else {
                        log.info("send message failed: " + detailRes.getErrMsg());
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    }

                    return detailRes;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return new DetailRes(false, "interrupted exception " + e.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                    retry(delivery, channel);
                    log.info("io exception : " + e);

                    return new DetailRes(false, "io exception " + e.toString());
                } catch (ShutdownSignalException e) {
                    e.printStackTrace();

                    try {
                        channel.close();
                    } catch (IOException io) {
                        io.printStackTrace();
                    } catch (TimeoutException timeout) {
                        timeout.printStackTrace();
                    }

                    consumer = buildQueueConsumer(connection, queue);

                    return new DetailRes(false, "shutdown exception " + e.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                    log.info("exception : " + e);
                    retry(delivery, channel);

                    return new DetailRes(false, "exception " + e.toString());
                }
            }
        };
    }

    private void retry(QueueingConsumer.Delivery delivery, Channel channel) {
        try {
            if (null != delivery) {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.info("send io exception " + e);
        }
    }

    private void buildQueue(String exchange, String routingKey,
                            final String queue, Connection connection) throws IOException {
        Channel channel = connection.createChannel(false);
        channel.exchangeDeclare(exchange, "direct", true, false, null);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, routingKey);

        try {
            channel.close();
        } catch (TimeoutException e) {
            e.printStackTrace();
            log.info("close channel time out " + e);
        }
    }

    private QueueingConsumer buildQueueConsumer(Connection connection, String queue) {
        Channel channel = connection.createChannel(false);
        QueueingConsumer consumer = new QueueingConsumer(channel);

        try {
            //通过 BasicQos 方法设置prefetchCount = 1。这样RabbitMQ就会使得每个Consumer在同一个时间点最多处理一个Message。
            //换句话说，在接收到该Consumer的ack前，他它不会将新的Message分发给它
            channel.basicQos(1);
            channel.basicConsume(queue, false, consumer);
        } catch (IOException e) {
            e.printStackTrace();
            log.info("build queue consumer error : " + e);
        }

        return consumer;
    }

    //for test
    public int getMessageCount(final String queue) throws IOException {
        Connection connection = connectionFactory.createConnection();
        final Channel channel = connection.createChannel(false);

        final AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queue);

        return declareOk.getMessageCount();
    }
}
