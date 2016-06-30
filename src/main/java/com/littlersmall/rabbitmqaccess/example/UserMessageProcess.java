package com.littlersmall.rabbitmqaccess.example;

import com.littlersmall.rabbitmqaccess.MessageProcess;
import com.littlersmall.rabbitmqaccess.common.DetailRes;
import org.springframework.stereotype.Service;

/**
 * Created by littlersmall on 16/6/28.
 */
public class UserMessageProcess implements MessageProcess<UserMessage> {
    @Override
    public DetailRes process(UserMessage userMessage) {
        System.out.println(userMessage);

        return new DetailRes(true, "");
    }
}
