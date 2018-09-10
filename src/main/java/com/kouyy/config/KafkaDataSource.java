package com.kouyy.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class KafkaDataSource {

    @Autowired
    private KafkaProducerConfig logProducerConfig;

    @Bean(name="logProducer")
    public KafkaTemplate<String, String> KafkaTemplate() {
        return new KafkaTemplate<String, String>( new DefaultKafkaProducerFactory<>(logProducerConfig.producerConfigs()));
    }
}
