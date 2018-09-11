package com.kouyy.consumer1;
 
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaUtil {
    private static final Logger logger = LogManager.getLogger(KafkaUtil.class);

    /**
     * 找一个broker leader
     * @param seedBrokers 配置的broker列表
     * @param port broker端口
     * @param topic
     * @param partition
     * @return
     */
    public static PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic, int partition) {
        PartitionMetadata returnMeataData = null;
        logger.info("find leader begin. brokers:[" + seedBrokers.toString() + "]");
        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse res = consumer.send(req);

                List<TopicMetadata> metadatas = res.topicsMetadata();
                for (TopicMetadata item : metadatas) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMeataData = part;
                            break loop;
                        }
                    }
 
                }
            } catch (Exception e) {
                logger.error("error communicating with broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        if (returnMeataData != null) {
            // TODO
            // seedBrokers.clear();
            // for (kafka.cluster.Broker seed : returnMeataData.replicas()) {
            //    seedBrokers.add(seed.host());
            // }
        }
        return returnMeataData;
    }
 
    /**
     * 找一个新的kafka broker leader
     * @param oldLeader
     * @param seedBrokers
     * @param port
     * @param topic
     * @param partition
     * @return
     * @throws Exception
     */
    public static String findNewLeader(String oldLeader,
                                       List<String> seedBrokers, int port,
                                       String topic, int partition) throws Exception {
        for (int i = 0; i < 3; ++i) {
            boolean sleep = false;
            PartitionMetadata metadata = findLeader(seedBrokers, port, topic, partition);
            if (metadata == null) {
                sleep = true;
            } else if (metadata.leader() == null) {
                sleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                sleep = true;
            } else {
                return metadata.leader().host();
            }
            if (sleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }
        logger.warn("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
 
    /**
     * 获取指定topic，指定partition的offset
     * @param consumer
     * @param topic
     * @param partition
     * @param whichTime
     * @param clientId
     * @return
     */
    public static long getSpecificOffset(SimpleConsumer consumer,
                                         String topic, int partition,
                                         long whichTime, String clientId) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request =
                new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            logger.warn("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return -1;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
}
