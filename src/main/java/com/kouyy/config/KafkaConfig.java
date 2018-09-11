package com.kouyy.config;

import java.util.List;

public class KafkaConfig {
 
    public String topic = null;                 // topic
    public int partitionNum = 0;                // partition个数
    public int port = 0;                        // kafka broker端口号
    public List<String> replicaBrokers = null;  // kafka broker ip列表
 
    public String checkpoint;                   // checkpoint目录，即保存partition offset的目录
    public int patchSize = 10;                  // 一次读取partition最大消息个数
 
    public String subscribeStartPoint = null;   // 默认开始订阅点，latest or earliest(最近或者最早)
    public String zookeeper = null;

    public KafkaConfig() { }
 
    @Override
    public String toString() {
        return "[brokers:" + replicaBrokers.toString()
                + "] [port:" + port
                + "] [topic:" + topic
                + "] [partition num:" + partitionNum
                + "] [patch size:" + patchSize
                + "] [start point:" + subscribeStartPoint
                + "] [zookeeperList:" + zookeeper
                + "]";
    }
}