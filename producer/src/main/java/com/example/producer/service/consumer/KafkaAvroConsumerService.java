package com.example.producer.service.consumer;

import com.example.producer.config.ConsumerAvroConfiguration;
import org.springframework.stereotype.Service;
import static com.example.producer.avro.Constants.TOPIC;

@Service
public class KafkaAvroConsumerService extends ConsumerAvroConfiguration {
    @Override
    protected String groupId() {
        return "MY-GROUP";
    }

    @Override
    protected String topic() {
        return TOPIC;
    }
}
