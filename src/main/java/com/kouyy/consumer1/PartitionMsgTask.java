package com.kouyy.consumer1;

import com.kouyy.config.KafkaConfig;
import com.kouyy.zookeeper.ZkClientUtils;
import com.kouyy.zookeeper.ZookeeperClient;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * kafka的simpaleConsumer，run()里面描述了消费消息的方法，但是出现异常程序就会终止
 */
public class PartitionMsgTask implements Runnable {
    private static final Logger logger = LogManager.getLogger(PartitionMsgTask.class);
//    private KafkaConsumerWork consumerWork;
    private String filePath = null;
    private int partitionIndex = 0;
    private SimpleConsumer consumer = null;
    private String leadBroker = null;
    private long readOffset = 0L;
    private String clientName = null;
    private String topic = null;
    private ZookeeperClient zkClient = null;
    private int port = 0;
    private int patchSize = 0;
    private String subscribeStartPoint = null;
    public List<String> replicaBrokers = null;

    public PartitionMsgTask(KafkaConfig config, int index) {
//        this.consumerWork = ((KafkaConsumerWork) SpringContextUtil.getBean("kafkaConsumerWork"));
        KafkaConfig kafkaConfig = config;
        this.partitionIndex = index;
        this.topic = kafkaConfig.topic;
        this.port = kafkaConfig.port;
        this.patchSize = kafkaConfig.patchSize;
        this.subscribeStartPoint = kafkaConfig.subscribeStartPoint;
        this.replicaBrokers = kafkaConfig.replicaBrokers;
        this.clientName = ("Client_" + this.topic + "_" + this.partitionIndex);
        this.filePath = "/" + this.topic + "/" + "partition" + this.partitionIndex;
        try {
            zkClient = ZkClientUtils.getZkClient(kafkaConfig.zookeeper, kafkaConfig.checkpoint, null);
            String Node_Path = this.filePath;
            Stat stat = zkClient.exists(Node_Path);
            if (stat == null) {
                zkClient.createPersitentNode(Node_Path, Node_Path, false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

        public void shutdown() {
        if (this.consumer != null) {
            this.consumer.close();
        }
        }

    @Override
    public void run() {
        Thread.currentThread().setName("Thread-" + this.topic + "-partition" + this.partitionIndex);
        PartitionMetadata metadata = KafkaUtil.findLeader(this.replicaBrokers, this.port, this.topic, this.partitionIndex);
        logger.info(" topic:" + this.topic + " partition:" + this.partitionIndex + " run success");
        if (metadata == null) {
            logger.error("Can't find metadata for Topic:" + this.topic + " and Partition:" + this.partitionIndex + ". Exiting");
            return;
        }
        if (metadata.leader() == null) {
            logger.error("Can't find Leader for Topic:" + this.topic + " and Partition:" + this.partitionIndex + ". Exiting");

            return;
        }
        this.leadBroker = metadata.leader().host();

        logger.info("leadBroker:" + this.leadBroker + " client" + this.clientName);
        // 连接leader节点构建具体的SimpleConsumer对象
        this.consumer = new SimpleConsumer(this.leadBroker, this.port, 100000, 65536, this.clientName);

        this.readOffset = getOffset(false);
        logger.info("first time get  offset :" + this.topic + ":" + this.partitionIndex + ":" + this.readOffset);
        if (this.readOffset == -1L) {
            logger.error("get offset failed");
            return;
        }
        //logger.info(this.kafkaConfig.topic + "_" + this.partitionIndex + " thread run success.");
        List<String> messageList = new ArrayList<String>(this.patchSize);
        try {
            while (true) {
                long offset = subsribe(this.patchSize, messageList);
                if (offset < 0L) {
                    logger.warn("subscribe message failed. will continue");
                } else {
                    // todo process messageList
                    // todo 如果处理失败，可重试或者继续，自己选择是否保存offset
                    this.readOffset = offset;
                    logger.info("Offset:" + this.topic + ":" + this.partitionIndex + ":" + readOffset);

                    int ret = saveOffset(offset);
                    if (ret != 0) {
                        if (saveOffset(offset) != 0) {
                            continue;
                        }
                    }
                    if (messageList != null) {
                        for (int i = 0; i < messageList.size(); i++) {
                            String message = (String) messageList.get(i);
                            // logger.info(message);
//                            this.consumerWork.Work(message);
                            System.out.println(message);
                        }
                    }
                }
                messageList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("exception :", e);
        }
    }

    /**
     * 获取offset
     *
     * @return
     */
    public long getOffset(boolean ignoreZookeper) {
        long offset = -1L;
        if (!ignoreZookeper) {
            String offsetFile = this.filePath + "/offset";
            try {
                Stat stat = zkClient.exists(offsetFile);
                if (stat == null) {
                    logger.info("offset file:" + offsetFile + " not found. will get the " + this.subscribeStartPoint + " offset.");
                } else {
                    String tempStr = zkClient.getNodeData(offsetFile, false);
                    offset = Long.parseLong(tempStr);
                    return offset;
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("get offset from  exception" + e.toString());
            }
        }
        if (this.subscribeStartPoint.equals("earliest")) {
            offset = KafkaUtil.getSpecificOffset(this.consumer, this.topic, this.partitionIndex, OffsetRequest.EarliestTime(), this.clientName);
        } else if (this.subscribeStartPoint.equals("latest")) {
            offset = KafkaUtil.getSpecificOffset(this.consumer, this.topic, this.partitionIndex, OffsetRequest.LatestTime(), this.clientName);
        } else {
            logger.error("kafka config start point error");
        }
        return offset;
//        if (!ignoreZookeper) {
//            return zook_offset;
//        } else {
//            if (zook_offset >=offset) {
//                zook_offset = offset;
//                return zook_offset;
//            } else {
//                return offset;
//            }
//        }
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1)); //build offset fetch request info
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
                OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request); //取到offsets

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition); //取到的一组offset
        return offsets[0]; //取第一个开始读
    }

    /**
     * 保持offset
     *
     * @param offset
     * @return
     */
    public int saveOffset(long offset) {

        String offsetFile = this.filePath + "/offset";
        try {
            Stat stat = zkClient.exists(offsetFile);
            if (stat == null) {
                zkClient.createPersitentNode(offsetFile, offset + "", false);
                return 0;
            } else {
                zkClient.setNodeData(offsetFile, offset + "");
                return 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("save offset failed" + e.toString());
            return -1;
        }
//        try {
//            File file = new File(offsetFile);
//            System.out.println(file.getPath());
//            if (!file.exists()) {
//                file.createNewFile();
//            }
//            FileWriter fileWriter = new FileWriter(file);
//            fileWriter.write(String.valueOf(offset));
//            fileWriter.close();
//        } catch (IOException e) {
//            logger.error("save offset failed");
//            return -1;
//        }

    }

    /**
     * 订阅消息
     *
     * @param maxReads
     * @param messageList
     * @return
     * @throws Exception
     */
    public long subsribe(long maxReads, List<String> messageList)
            throws Exception {
        if (messageList == null) {
            logger.warn("messageList is null");
            return -1L;
        }
        int numErrors = 0;
        long offset = this.readOffset;
        while (maxReads > 0L) {
            if (this.consumer == null) {
                this.consumer = new SimpleConsumer(this.leadBroker, this.port, 100000, 65536, this.clientName);
            }
            FetchRequest request = new FetchRequestBuilder().clientId(this.clientName).addFetch(this.topic, this.partitionIndex, offset, 100000).build();
            FetchResponse fetchResponse = this.consumer.fetch(request);
            if (fetchResponse.hasError()) {
                //logger.warn("fetch response has error");
                numErrors++;
                short code = fetchResponse.errorCode(this.topic, this.partitionIndex);
                logger.warn("Error fetching data from the Broker:" + this.topic + ":" + this.partitionIndex + ":" + offset + " the leadBroker " + this.leadBroker + " error code: " + code);
                if (numErrors > 3) {
                    return -1L;
                }
                // 处理offset非法的问题，用最新的offset
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    logger.warn("offset out of range.the  OutOfRange offset is " + this.topic + ":" + this.partitionIndex + ":" + offset);
                    offset = getOffset(true);
                    logger.warn("offset out of range. has get a new offset " + this.topic + ":" + this.partitionIndex + ":" + offset + " the leadBroker " + this.leadBroker);
                    continue;
                }
                this.consumer.close();
                this.consumer = null;
                // 更新leader broker
                this.leadBroker = KafkaUtil.findNewLeader(leadBroker, this.replicaBrokers, this.port, this.topic, partitionIndex);
                continue;
            } else {
                numErrors = 0;
                long numRead = 0L;
                for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(this.topic, this.partitionIndex)) {
                    long currentOffset = messageAndOffset.offset();
                    // 必要判断，因为对于compressed message，会返回整个block，所以可能包含old的message
                    if (currentOffset < offset) {
                        logger.warn("Found an old offset: " + currentOffset + " Expecting: " + offset);
                    } else {
                        // 获取下一个readOffset
                        offset = messageAndOffset.nextOffset();
                        ByteBuffer payload = messageAndOffset.message().payload();

                        byte[] bytes = new byte[payload.limit()];
                        payload.get(bytes);
                        String message = new String(bytes, "UTF-8");
                        messageList.add(message);
                        numRead += 1L;
                        maxReads -= 1L;
                    }
                }
                if (numRead == 0L) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException localInterruptedException1) {

                    }
                }
            }
        }
        return offset;
    }
//    /**
//     * 从保存consumer消费者offset偏移量的位置获取当前consumer对应的偏移量
//     *
//     * @param consumer    消费者
//     * @param groupId     Group Id
//     * @param clientName  client名称
//     * @param topic       topic名称
//     * @param partitionID 分区id
//     * @return
//     */
//    public long getOffsetOfTopicAndPartition(SimpleConsumer consumer, String groupId, String clientName, String topic, int partitionID) {
//        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionID);
//        List<TopicAndPartition> requestInfo = new ArrayList<TopicAndPartition>();
//        requestInfo.add(topicAndPartition);
//        kafka.javaapi.OffsetFetchRequest request = new kafka.javaapi.OffsetFetchRequest(groupId, requestInfo, 0, clientName);
//        kafka.javaapi.OffsetFetchResponse response = consumer.fetchOffsets(request);
//
//        // 获取返回值
//        Map<TopicAndPartition, OffsetMetadataAndError> returnOffsetMetadata = response.offsets();
//        // 处理返回值
//        if (returnOffsetMetadata != null && !returnOffsetMetadata.isEmpty()) {
//            // 获取当前分区对应的偏移量信息
//            OffsetMetadataAndError offset = returnOffsetMetadata.get(topicAndPartition);
//            if (offset.error() == ErrorMapping.NoError()) {
//                // 没有异常，表示是正常的，获取偏移量
//                return offset.offset();
//            } else {
//                // 当Consumer第一次连接的时候(zk中不在当前topic对应数据的时候)，会产生UnknownTopicOrPartitionCode异常
//                System.out.println("Error fetching data Offset Data the Topic and Partition. Reason: " + offset.error());
//            }
//        }
//
//        // 所有异常情况直接返回0
//        return 0;
//    }
//
//    /**
//     * 更新偏移量，当SimpleConsumer发生变化的时候，重新构造一个新的SimpleConsumer并返回
//     *
//     * @param consumer
//     * @param topic
//     * @param partitionID
//     * @param
//     * @param groupId
//     * @param clientName
//     * @param times
//     * @return
//     * @throws RuntimeException 当更新失败的情况下
//     */
//    private SimpleConsumer updateOffset(SimpleConsumer consumer, String topic, int partitionID, long , String groupId, String clientName, int times) {
//        // 构建请求对象
//        Map<TopicAndPartition, OffsetAndMetadata> requestInfoMap = new HashMap<TopicAndPartition, OffsetAndMetadata>();
//        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionID);
//        requestInfoMap.put(topicAndPartition, new OffsetAndMetadata(readOffSet, OffsetAndMetadata.NoMetadata(), -1));
//        kafka.javaapi.OffsetCommitRequest ocRequest = new OffsetCommitRequest(groupId, requestInfoMap, 0, clientName);
//        // 提交修改偏移量的请求，并获取返回值
//        kafka.javaapi.OffsetCommitResponse response = consumer.commitOffsets(ocRequest);
//
//        // 根据返回值进行不同的操作
//        if (response.hasError()) {
//            short code = response.errorCode(topicAndPartition);
//            if (times > this.maxRetryTimes) {
//                throw new RuntimeException("Update the Offset occur exception," +
//                        " the current response code is:" + code);
//            }
//
//            if (code == ErrorMapping.LeaderNotAvailableCode()) {
//                // 当异常code为leader切换情况的时候，重新构建consumer对象
//                // 操作步骤：先休眠一段时间，再重新构造consumer对象，最后重试
//                try {
//                    Thread.sleep(this.retryIntervalMillis);
//                } catch (InterruptedException e) {
//                    // nothings
//                }
//                PartitionMetadata metadata = this.findNewLeaderMetadata(consumer.host(), topic, partitionID);
//                this.validatePartitionMetadata(metadata);
//                // consumer = this.createSimpleConsumer(metadata.leader().host(), metadata.leader().port(), clientName);
//                this.leadBroker = metadata.leader().host();
//                consumer = new SimpleConsumer(this.leadBroker, this.kafkaConfig.port, 100000, 65536, this.clientName);
//                // 重试
//                consumer = updateOffset(consumer, topic, partitionID, readOffSet, groupId, clientName, times + 1);
//            }
//
//            if (code == ErrorMapping.RequestTimedOutCode()) {
//                // 当异常为请求超时的时候，进行重新请求
//                consumer = updateOffset(consumer, topic, partitionID, readOffSet, groupId, clientName, times + 1);
//            }
//
//            // 其他code直接抛出异常
//            throw new RuntimeException("Update the Offset occur exception," +
//                    " the current response code is:" + code);
//        }
//
//        // 返回修改后的consumer对象
//        return consumer;
//    }
//
//    /**
//     * 根据给定参数获取一个新leader的分区元数据信息
//     *
//     * @param oldLeader
//     * @param topic
//     * @param partitionID
//     * @return
//     */
//    private PartitionMetadata findNewLeaderMetadata(String oldLeader,
//                                                    String topic,
//                                                    int partitionID) {
//        for (int i = 0; i < 3; i++) {
//            boolean gotoSleep = false;
//            PartitionMetadata metadata = KafkaUtil.findLeader(this.kafkaConfig.replicaBrokers, this.kafkaConfig.port, this.kafkaConfig.topic, this.partitionIndex);
//            if (metadata == null) {
//                gotoSleep = true;
//            } else if (metadata.leader() == null) {
//                gotoSleep = true;
//            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
//                // leader切换过程中
//                gotoSleep = true;
//            } else {
//                return metadata;
//            }
//
//            if (gotoSleep) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    // nothings
//                }
//            }
//        }
//
//        System.out.println("Unable to find new leader after Broker failure. Exiting!!");
//        throw new RuntimeException("Unable to find new leader after Broker failure. Exiting!!");
//    }
//
//    /**
//     * 验证分区元数据，如果验证失败，直接抛出IllegalArgumentException异常
//     *
//     * @param metadata
//     */
//    private void validatePartitionMetadata(PartitionMetadata metadata) {
//        if (metadata == null) {
//            System.out.println("Can't find metadata for Topic and Partition. Exiting!!");
//            throw new IllegalArgumentException("Can't find metadata for Topic and Partition. Exiting!!");
//        }
//        if (metadata.leader() == null) {
//            System.out.println("Can't find Leader for Topic and Partition. Exiting!!");
//            throw new IllegalArgumentException("Can't find Leader for Topic and Partition. Exiting!!");
//        }
//    }


}