package com.littlersmall.rabbitmqaccess.common;

/**
 * Created by littlersmall on 16/5/19.
 */
public class Constants {
    //线程数
    public final static int THREAD_COUNT = 5;

    //处理间隔时间
    //mils
    public final static int INTERVAL_MILS = 0;

    //consumer失败后等待时间(mils)
    public static final int ONE_SECOND = 1 * 1000;

    //异常sleep时间(mils)
    public static final int ONE_MINUTE = 1 * 60 * 1000;
    //MQ消息retry时间
    public static final int RETRY_TIME_INTERVAL = ONE_MINUTE;
    //MQ消息有效时间
    public static final int VALID_TIME = ONE_MINUTE;
}
