package com.littlersmall.rabbitmqaccess;

import com.littlersmall.rabbitmqaccess.common.DetailRes;

/**
 * Created by littlersmall on 16/5/12.
 */
public interface MessageSender {
    DetailRes send(Object message);
}
