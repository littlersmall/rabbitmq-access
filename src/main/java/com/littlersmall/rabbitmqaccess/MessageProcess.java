package com.littlersmall.rabbitmqaccess;


import com.littlersmall.rabbitmqaccess.common.DetailRes;

/**
 * Created by littlersmall on 16/5/11.
 */
public interface MessageProcess<T> {
    DetailRes process(T message);
}
