![rabbitmq.png](http://upload-images.jianshu.io/upload_images/1397675-8f305b180a895baf.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**20180927 更新**

升级 retryCache的容器，修改为 ConcurrentSkipListMap

1 retry的时候按先后顺序尝试

2 hashMap无法自动缩容，在rabbitmq出现问题时，map造成积压，等问题恢复后，map的多余空间无法自动释放，而SkipListMap可以完美避开这个问题

3 在大量插入删除时，SkipList的效率更高

---

**20180710 更新**

1 升级spring-rabbit版本，升级到最新版本

2 去除对QueueConsumer的使用，改为使用basicGet方法(消费效率和原来的方式对比，有微弱提升)

3 改进一些打log的细节

---

**20180517 更新**

1 retryCache重构，解决rabbitmq挂掉时消息积压的问题

2 部分细节改进

---

**20171120 更新**

1 改进一些细节：遍历map时基于entry，增加一定的效率

---

**20170510 更新**

1 增加线程池consumer优雅退出机制Runtime.getRuntime().addShutdownHook

2 修改部分log输出方式，将原来的 log.info("exceptin:" + e) 修复为 log.info("exception: ", e)

---

**20161227 更新**

1 bug fix: 将messageProcess包裹在try，catch中，避免队列中出现unack的死信息

2 bug分析见http://www.jianshu.com/p/a7edc3322b44

---

**20161205 更新**

1 增加topic模式

2 原有的使用direct方式无需更改，本次为兼容性升级，增加了buildTopicMessageSender和buildTopicMessageConsumer方法

3 ThreadPoolConsumer默认为direct方式，可以通过setType("topic")修改为topic模式

---

**20160907 更新**

1 解决因网络抖动而引起的发送数据丢失

2 增加retry模块

3 在本地缓存已发送数据，根据ack的确认将已ack的删除

4 定时触发重发未收到ack的数据

5 保证在网络抖动的情况下数据不丢失，但可能会造成数据的重复发送(建议在consumer端做到message处理的幂等性)

---


最近的一个计费项目，在rpc调用和流式处理之间徘徊了许久，后来选择流式处理。一是可以增加吞吐量，二是事务的控制相比于rpc要容易很多。
确定了流式处理的方式，后续是技术的选型。刚开始倾向于用storm，无奈文档实在太少，折腾起来着实费劲。最终放弃，改用消息队列+微服务的方式实现。

消息队列的选型上，有activemq，rabbitmq，kafka等。最开始倾向于用activemq，因为以前的项目用过，很多代码都是可直接复用的。后来看了不少文章对比，发现rabbitmq对多语言的支持更好一点，同时相比于kafka，牺牲了部分的性能换取了更好的稳定性安全性以及持久化。
最终决定使用rabbitmq。

rabbitmq的官网如下：
>https://www.rabbitmq.com/

对rabbitmq的封装，有几个目标：
1 提供send接口
2 提供consume接口
3 保证消息的事务性处理

所谓事务性处理，是指对一个消息的处理必须严格可控，必须满足原子性，只有两种可能的处理结果：
(1) 处理成功，从队列中删除消息
(2) 处理失败(网络问题，程序问题，服务挂了)，将消息重新放回队列
为了做到这点，我们使用rabbitmq的手动ack模式，这个后面细说。

**1 send接口**
```
public interface MessageSender {    
    DetailRes send(Object message);
}
```
send接口相对简单，我们使用spring的RabbitTemplate来实现，代码如下：
```
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
```
**2 consume接口**
```
public interface MessageConsumer {    
    DetailRes consume();
}
```
在consume接口中，会调用用户自己的MessageProcess，接口定义如下：
```
public interface MessageProcess<T> {    
    DetailRes process(T message);
}
```
consume的实现相对来说复杂一点，代码如下：
```
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
```
**3 保证消息的事务性处理**
rabbitmq默认的处理方式为auto ack，这意味着当你从消息队列取出一个消息时，ack自动发送，mq就会将消息删除。而为了保证消息的正确处理，我们需要将消息处理修改为手动确认的方式。
(1) sender的手工确认模式 
首先将ConnectionFactory的模式设置为publisherConfirms，如下
```
connectionFactory.setPublisherConfirms(true);
```
之后设置rabbitTemplate的confirmCallback，如下：
```
rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
       if (!ack) {
           log.info("send message failed: " + cause); //+ correlationData.toString());
           throw new RuntimeException("send error " + cause);
       }
    }
});
```
(2) consume的手工确认模式
首先在queue创建中指定模式
```
channel.exchangeDeclare(exchange, "direct", true, false, null);
/**
 * Declare a queue
 * @see com.rabbitmq.client.AMQP.Queue.Declare
 * @see com.rabbitmq.client.AMQP.Queue.DeclareOk
 * @param queue the name of the queue
 * @param durable true if we are declaring a durable queue (the queue will survive a server restart)
 * @param exclusive true if we are declaring an exclusive queue (restricted to this connection)
 * @param autoDelete true if we are declaring an autodelete queue (server will delete it when no longer in use)
 * @param arguments other properties (construction arguments) for the queue
 * @return a declaration-confirm method to indicate the queue was successfully declared
 * @throws java.io.IOException if an error is encountered
 */
channel.queueDeclare(queue, true, false, false, null);
```
只有在消息处理成功后发送ack确认，或失败后发送nack使信息重新投递
```
if (detailRes.isSuccess()) {
    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
} else {
    log.info("send message failed: " + detailRes.getErrMsg());
    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
}
```
**4 自动重连机制**
为了保证rabbitmq的高可用性，我们使用rabbitmq Cluster模式，并配合haproxy。这样，在一台机器down掉时或者网络发生抖动时，就会发生当前连接失败的情况，如果不对这种情况做处理，就会造成当前的服务不可用。
在spring-rabbitmq中，已实现了connection的自动重连，但是connection重连后，channel的状态并不正确。因此我们需要自己捕捉ShutdownSignalException异常，并重新生成channel。如下：
```
catch (ShutdownSignalException e) {
    e.printStackTrace();
    channel.close();
    //recreate channel
    consumer = buildQueueConsumer(connection, queue);
}
```
**5 consumer线程池**
在对消息处理的过程中，我们期望多线程并行执行来增加效率，因此对consumer做了一个线程池的封装。
线程池通过builder模式构造，需要准备如下参数：
```
//线程数量
int threadCount;
//处理间隔(每个线程处理完成后休息的时间)
long intervalMils;
//exchange及queue信息
String exchange;
String routingKey;
String queue;
//用户自定义处理接口
MessageProcess<T> messageProcess;
```
核心循环也较为简单，代码如下：
```
public void run() {
    while (!stop) {
        try {
            //2
            DetailRes detailRes = messageConsumer.consume();

            if (infoHolder.intervalMils > 0) {
                try {
                    Thread.sleep(infoHolder.intervalMils);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    log.info("interrupt " + e);
                }
            }

            if (!detailRes.isSuccess()) {
                log.info("run error " + detailRes.getErrMsg());
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.info("run exception " + e);
        }
    }
}
```
**6 使用示例**
最后，我们还是用一个例子做结。
(1) 定义model
```
//参考lombok
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserMessage {
    int id;
    String name;
}
```
(2) rabbitmq配置
配置我们使用@Configuration实现，如下：
```
@Configuration
public class RabbitMQConf {
    @Bean
    ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory("127.0.0.1", 5672);

        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        connectionFactory.setPublisherConfirms(true); // enable confirm mode

        return connectionFactory;
    }
}
```
(3) sender示例
```
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
```
(4) MessageProcess(用户自定义处理接口)示例，本例中我们只是简单的将信息打印出来
```
public class UserMessageProcess implements MessageProcess<UserMessage> {
    @Override
    public DetailRes process(UserMessage userMessage) {
        System.out.println(userMessage);

        return new DetailRes(true, "");
    }
}
```
(5) consumer示例
```
@Service
public class ConsumerExample {
    private static final String EXCHANGE = "example";
    private static final String ROUTING = "user-example";
    private static final String QUEUE = "user-example";

    @Autowired
    ConnectionFactory connectionFactory;

    private MessageConsumer messageConsumer;

    @PostConstruct
    public void init() throws IOException, TimeoutException {
        MQAccessBuilder mqAccessBuilder = new MQAccessBuilder(connectionFactory);
        messageConsumer = mqAccessBuilder.buildMessageConsumer(EXCHANGE, ROUTING, QUEUE, new UserMessageProcess());
    }

    public DetailRes consume() {
        return messageConsumer.consume();
    }
}
```
(6) 线程池consumer示例
在main函数中，我们使用一个独立线程发送数据，并使用线程池接收数据。
```
@Service
public class PoolExample {
    private static final String EXCHANGE = "example";
    private static final String ROUTING = "user-example";
    private static final String QUEUE = "user-example";

    @Autowired
    ConnectionFactory connectionFactory;

    private ThreadPoolConsumer<UserMessage> threadPoolConsumer;

    @PostConstruct
    public void init() {
        MQAccessBuilder mqAccessBuilder = new MQAccessBuilder(connectionFactory);
        MessageProcess<UserMessage> messageProcess = new UserMessageProcess();

        threadPoolConsumer = new ThreadPoolConsumer.ThreadPoolConsumerBuilder<UserMessage>()
                .setThreadCount(Constants.THREAD_COUNT).setIntervalMils(Constants.INTERVAL_MILS)
                .setExchange(EXCHANGE).setRoutingKey(ROUTING).setQueue(QUEUE)
                .setMQAccessBuilder(mqAccessBuilder).setMessageProcess(messageProcess)
                .build();
    }

    public void start() throws IOException {
        threadPoolConsumer.start();
    }

    public void stop() {
        threadPoolConsumer.stop();
    }

    public static void main(String[] args) throws IOException {
        ApplicationContext ac = new ClassPathXmlApplicationContext("applicationContext.xml");
        PoolExample poolExample = ac.getBean(PoolExample.class);
        final SenderExample senderExample = ac.getBean(SenderExample.class);

        poolExample.start();

        new Thread(new Runnable() {
            int id = 0;

            @Override
            public void run() {
                while (true) {
                    senderExample.send(new UserMessage(id++, "" + System.nanoTime()));

                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }
}
```
**7 github地址，路过的帮忙点个星星，谢谢^_^。**
>https://github.com/littlersmall/rabbitmq-access

附：
rabbitmq安装过程：
mac版安装可以使用homebrew。brew install就可以，安装好之后通过brew services start rabbitmq启动服务。通过
>http://localhost:15672/#/

就可以在页面端看到rabbitmq了，如下：
![rabbitmq_manager.png](http://upload-images.jianshu.io/upload_images/1397675-bcc34125a5bdaedf.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**have fun**



