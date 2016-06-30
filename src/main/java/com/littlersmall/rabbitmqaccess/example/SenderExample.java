package com.littlersmall.rabbitmqaccess.example;

import com.littlersmall.rabbitmqaccess.MQAccessBuilder;
import com.littlersmall.rabbitmqaccess.MessageSender;
import com.littlersmall.rabbitmqaccess.common.DetailRes;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by littlersmall on 16/6/28.
 */
@Service
public class SenderExample {
    private static final String EXCHANGE = "example";
    private static final String ROUTING = "user-example";
    private static final String QUEUE = "user-example";

    @Autowired
    ConnectionFactory connectionFactory;

    private MessageSender messageSender;

    @PostConstruct
    public void init() throws IOException, TimeoutException {
        MQAccessBuilder mqAccessBuilder = new MQAccessBuilder(connectionFactory);
        messageSender = mqAccessBuilder.buildMessageSender(EXCHANGE, ROUTING, QUEUE);
    }

    public DetailRes send(UserMessage userMessage) {
        return messageSender.send(userMessage);
    }
}
