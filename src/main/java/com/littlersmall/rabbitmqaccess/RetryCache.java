package com.littlersmall.rabbitmqaccess;

import com.littlersmall.rabbitmqaccess.common.Constants;
import com.littlersmall.rabbitmqaccess.common.DetailRes;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by littlersmall on 16/9/5.
 */
@Slf4j
public class RetryCache {
    private MessageSender sender;
    private boolean stop = false;
    private Map<String, MessageWithTime> map = new ConcurrentHashMap<>();
    private AtomicLong id = new AtomicLong();

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    private static class MessageWithTime {
        long time;
        Object message;
    }

    public void setSender(MessageSender sender) {
        this.sender = sender;
        startRetry();
    }

    public String generateId() {
        return "" + id.incrementAndGet();
    }

    public void add(String id, Object message) {
        map.put(id, new MessageWithTime(System.currentTimeMillis(), message));
    }

    public void del(String id) {
        map.remove(id);
    }

    private void startRetry() {
        new Thread(() ->{
            while (!stop) {
                try {
                    Thread.sleep(Constants.RETRY_TIME_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                long now = System.currentTimeMillis();

                for (String key : map.keySet()) {
                    MessageWithTime messageWithTime = map.get(key);

                    if (null != messageWithTime) {
                        if (messageWithTime.getTime() + 3 * Constants.VALID_TIME < now) {
                            log.info("send message failed after 3 min " + messageWithTime);
                            del(key);
                        } else if (messageWithTime.getTime() + Constants.VALID_TIME < now) {
                            DetailRes detailRes = sender.send(messageWithTime.getMessage());

                            if (detailRes.isSuccess()) {
                                del(key);
                            }
                        }
                    }
                }
            }
        }).start();
    }
}
