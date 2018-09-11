package com.kouyy.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

@Configuration
@PropertySources({
        @PropertySource("classpath:config/kafka-consumer.properties"),
        @PropertySource(value = "${kafka-consumer.properties}", ignoreResourceNotFound = true)
})
public class KafkaConsumerConfig {
    @Value("${brokerList}")
    private String brokerList;
    @Value("${port}")
    private String port;
    @Value("${partitionNum}")
    private String partitionNum;
    @Value("${checkpoint}")
    private String checkpoint;
    @Value("${patchSize}")
    private String patchSize;
    @Value("${subscribeStartPoint}")
    private String subscribeStartPoint;
    @Value("${zookeeper}")
    private String zookeeper;
    @Value("${topics}")
    private String topics;

    public String getBrokerList() {
        return this.brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getPort() {
        return this.port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getPartitionNum() {
        return this.partitionNum;
    }

    public void setPartitionNum(String partitionNum) {
        this.partitionNum = partitionNum;
    }

    public String getCheckpoint() {
        return this.checkpoint;
    }

    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    public String getPatchSize() {
        return this.patchSize;
    }

    public void setPatchSize(String patchSize) {
        this.patchSize = patchSize;
    }

    public String getSubscribeStartPoint() {
        return this.subscribeStartPoint;
    }

    public void setSubscribeStartPoint(String subscribeStartPoint) {
        this.subscribeStartPoint = subscribeStartPoint;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public String getTopics() {
        return topics;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }
}
