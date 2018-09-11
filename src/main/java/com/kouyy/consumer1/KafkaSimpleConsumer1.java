package com.kouyy.consumer1;
 
import com.kouyy.config.KafkaConfig;
import com.kouyy.config.KafkaConsumerConfig;
import com.kouyy.zookeeper.ZkClientUtils;
import com.kouyy.zookeeper.ZookeeperClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 指定topic的消费者
 */
public class KafkaSimpleConsumer1 {
    private static final Logger logger = LogManager.getLogger(KafkaSimpleConsumer1.class);
 
    private KafkaConfig kafkaConfig = null;
    private ExecutorService executor = null;
    private boolean inited = false;
    private String path = null;
    private int partitionNum = 0;

    public KafkaSimpleConsumer1(KafkaConsumerConfig config, String topic) {
        this.kafkaConfig = new KafkaConfig();
        init(config,topic);
    }

    public void run() {
        if (!inited) {
            logger.error("uninit, init first!");
            return;
        }
        this.executor = Executors.newFixedThreadPool(this.partitionNum);
            for (int threadNum = 0; threadNum < this.partitionNum; threadNum++) {
                this.executor.execute(new PartitionMsgTask(this.kafkaConfig, threadNum));
            }
    }
 
    /*public int init(String confFile) {
        Properties props = new Properties();
        kafkaConfig = new KafkaConfig();
        try {
            FileInputStream in = new FileInputStream(confFile);
            props.load(in);
        } catch (FileNotFoundException e) {
            logger.error("kafka config file not found. file name:" + confFile);
            return -1;
        } catch (IOException e) {
            logger.error("properties load file failed");
            return -1;
        }
        kafkaConfig.topic = props.getProperty("topic");
        kafkaConfig.port = Integer.parseInt(props.getProperty("port"));
        kafkaConfig.partitionNum = Integer.parseInt(props.getProperty("partitionNum"));
        kafkaConfig.checkpoint = props.getProperty("checkpoint");
        kafkaConfig.patchSize = Integer.parseInt(props.getProperty("patchSize"));
        String startPoint = props.getProperty("subscribeStartPoint");
        if (!startPoint.equals("latest") && !startPoint.equals("earliest")) {
            logger.error("config file startPoint error. startPoint must be latest or earliest");
            return -1;
        }
        kafkaConfig.subscribeStartPoint = startPoint;
        String brokerList = props.getProperty("brokerList");
        String[] brokers = brokerList.split(",");
        kafkaConfig.replicaBrokers = new ArrayList<String>();
        for (String str : brokers) {
            kafkaConfig.replicaBrokers.add(str);
        }
        inited = true;
        logger.info("init success. kafkaConfig:" + kafkaConfig.toString());
        return 0;
    }*/

    public int init(KafkaConsumerConfig config, String topic) {
        //给kafkaConsumerConfig参数赋值
        this.partitionNum = Integer.parseInt(config.getPartitionNum());
        this.kafkaConfig.topic = topic;
        this.kafkaConfig.port = Integer.parseInt(config.getPort());
        this.kafkaConfig.checkpoint = config.getCheckpoint();
        this.kafkaConfig.patchSize = Integer.parseInt(config.getPatchSize());
        String startPoint = config.getSubscribeStartPoint();
        if ((!startPoint.equals("latest")) && (!startPoint.equals("earliest"))) {
            System.err.println("config file startPoint error. startPoint must be latest or earliest");
            return -1;
        }
        this.kafkaConfig.subscribeStartPoint = startPoint;
        this.kafkaConfig.zookeeper = config.getZookeeper();
        String brokerList = config.getBrokerList();
        String[] brokers = brokerList.split(",");
        this.kafkaConfig.replicaBrokers = new ArrayList();
        for (String str : brokers) {
            this.kafkaConfig.replicaBrokers.add(str);
        }
        //生成保存offset值的zookeeper节点
        this.path = "/" + this.kafkaConfig.topic;
        ZookeeperClient zkClient= null;
        try {
            zkClient = ZkClientUtils.getZkClient(this.kafkaConfig.zookeeper,this.kafkaConfig.checkpoint,null);
            String Node_Path=this.path;
            Stat stat= zkClient.exists(Node_Path);
            if(stat==null)
            {
                zkClient.createPersitentNode(Node_Path, Node_Path,false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.inited = true;
        return 0;
    }
 
}