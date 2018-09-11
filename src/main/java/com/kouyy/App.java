package com.kouyy;

import com.kouyy.config.KafkaConsumerConfig;
import com.kouyy.consumer1.KafkaSimpleConsumer1;
import com.kouyy.consumer2.KafkaSimpleConsumer2;
import com.kouyy.zookeeper.ZkClientUtils;
import com.kouyy.zookeeper.ZookeeperClient;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@ComponentScan(basePackages =
        {
                "com.kouyy"
        })
@SpringBootApplication
public class App  implements CommandLineRunner {

    @Autowired
    public KafkaConsumerConfig kafkaConsumerConfig;

    public static void main( String[] args ) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        String[] topic_list = kafkaConsumerConfig.getTopics().split(",");

        ZookeeperClient zkClient= ZkClientUtils.getZkClient(kafkaConsumerConfig.getZookeeper(),kafkaConsumerConfig.getCheckpoint(),null);
        String Node_Path="/";
        Stat stat= zkClient.exists(Node_Path);
        if(stat==null)
        {
            zkClient.createPersitentNode(Node_Path, Node_Path,false);
        }
        ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(1);

        for (int i = 0; i < topic_list.length; i++) {
            KafkaSimpleConsumer1 consumer1 = new KafkaSimpleConsumer1(this.kafkaConsumerConfig, topic_list[i]);
            KafkaSimpleConsumer2 consumer2 = new KafkaSimpleConsumer2(this.kafkaConsumerConfig, topic_list[i]);
            consumer1.run();
            consumer2.run();
        }
    }
}
