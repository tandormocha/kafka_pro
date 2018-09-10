package com.kouyy.producer;

import com.alibaba.fastjson.JSON;
import com.kouyy.ILogService;
import com.kouyy.model.KafkaMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 生产和消费消息实现类
 */
@Service
public class LogServiceImpl implements ILogService {


    // 线程操作安全队列，装载数据
    private static final Queue<KafkaMessage> queue = new ConcurrentLinkedQueue<KafkaMessage>();

    private int maxLength = 1024 * 800;

    @Autowired
    @Qualifier("logProducer")
    private KafkaTemplate logKafkaTemplate;

    //@PostConstruct表示该方法只会发生在构造函数之后
    @PostConstruct
    private void consumerMessage() {
        //System.out.println("consumerMessage 被执行");
        // 消费者线程：不断的消费队列中的数据
        // 该线程不停的从队列中取出队列中最头部的数据
        new Thread(new Runnable() {
            @Override
            public void run() {
                KafkaMessage kafkaMessage=null;
                String message=null;
                while (true) {
                    try {
                        if (queue.isEmpty()) {
                            Thread.sleep(2000);
                        } else {
                            // 从队列的头部取出并删除该条数据
                            kafkaMessage = queue.poll();
                            if (kafkaMessage != null) {
                                message = JSON.toJSONString(kafkaMessage);
                                logKafkaTemplate.send(kafkaMessage.getTopic(), message);
                            }
                            kafkaMessage=null;
                            message=null;
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }

        }).start();
    }

    private void sendMessage(KafkaMessage kafkaMessage) {
        queue.add(kafkaMessage);
//        String message = JSON.toJSONString(kafkaMessage);
//        kafkaTemplate.send(kafkaMessage.getTopic(), message);
    }

    /**
     * 保存异常消息
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型,通过这个类型获取异常是哪个模块最开始调用的类抛出来的
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     */
    @Override
    public void error(String message, String type, String topic, String group) {

        KafkaMessage kafkaMessage = getKafkaMessage(message, type, topic, group, "error");
        sendMessage(kafkaMessage);

    }

    /**
     * 保存异常消息
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型,通过这个类型获取异常是哪个模块最开始调用的类抛出来的
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     * @param ex      异常
     */
    @Override
    public void error(String message, String type, String topic, String group, Exception ex) {

        String errMsg = message + "\r\n" + getErrorInfoFromException(ex);
        error(errMsg, type, topic, group);

    }

    /**
     * 记录info日志
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     */
    @Override
    public void info(String message, String type, String topic, String group) {

        KafkaMessage kafkaMessage = getKafkaMessage(message, type, topic, group, "info");
        sendMessage(kafkaMessage);

    }

    /**
     * 记录warn日志
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     */
    @Override
    public void warn(String message, String type, String topic, String group) {

        KafkaMessage kafkaMessage = getKafkaMessage(message, type, topic, group, "warn");
        sendMessage(kafkaMessage);

    }

    /**
     * 记录fatal日志
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     */
    @Override
    public void fatal(String message, String type, String topic, String group) {
        KafkaMessage kafkaMessage = getKafkaMessage(message, type, topic, group, "fatal");
        sendMessage(kafkaMessage);
    }

    private KafkaMessage getKafkaMessage(String message, String type, String topic, String group, String level) {
        try {
            if (getBytes(message) > maxLength) {
                if (message.length() > 2000) {
                    message = message.substring(0, 1999);
                } else if (message.length() > 500) {
                    message = message.substring(0, 499);
                } else {
                    message = message.substring(0, message.length() / 2);
                }
            }
        } catch (Exception ex) {
            message = ex.getMessage();
        }

        KafkaMessage kafkaMessage = new KafkaMessage(message, level, type, group, topic);
        return kafkaMessage;
    }

    private String getErrorInfoFromException(Exception e) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return "\r\n" + sw.toString() + "\r\n";
        } catch (Exception e2) {
            return "bad getErrorInfoFromException";
        }
    }

    private int getBytes(String data) {
        try {
            byte[] bytes = data.getBytes("utf-8");
            return bytes.length;
        } catch (Exception ex) {
            return data.length();
        }
    }
}
