package com.kouyy.model;


import java.util.Date;

/**
 * kafka消息实体类，level为错误级别
 */
public class KafkaMessage {
    private String topic;
    private String type;
    private String message;
    private String level;
    private String host;
    private String group;
    private Long createDate;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Long getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Long createDate) {
        this.createDate = createDate;
    }

    public KafkaMessage(String message, String level, String type, String group, String topic) {
        this.message = message;
        this.level = level;
        this.type = type;
        this.group = group;
        this.topic = topic;
        this.createDate = new Date().getTime();

    }
}
