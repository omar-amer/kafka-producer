package com.example.producer.service.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class KafkaProducerService extends AbstractProducerService {

    private final KafkaProducer<String, Object> kafkaProducer;
    private final String TOPIC = "myorders";

    public KafkaProducerService(@Qualifier("kafkaProducer")
                                KafkaProducer<String, Object> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Scheduled(fixedDelay = 1000)
    public void sendMessage() {
        super.sendMessage();
    }
    @Override
    protected boolean isActive() {
        return false;
    }

    @Override
    protected void publish() {
        Random random = new Random();
        String key = "name" + random.nextInt();
        String value = "omar" + random.nextInt();
        Map<String, Object> messageObject = Map.of(key, value);

        var partition = new Random().nextInt(2 + 1);
        List<Header> headers = new ArrayList<>();
        headers.add(0, new RecordHeader("Title", "Lead".getBytes()));
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(TOPIC, partition, key, value, headers);
        String randomKey = String.valueOf(ThreadLocalRandom.current().nextInt(1, 2 + 1));
        System.out.println("Random key: " + randomKey);
        ProducerRecord<String, Object> headerlessProducerRecord = new ProducerRecord<>(TOPIC, randomKey, value);
        kafkaProducer.send(headerlessProducerRecord, (metadata, e) -> {
            if (Objects.nonNull(metadata)) {
                System.out.println("Key: " + headerlessProducerRecord.key());
                System.out.println("Value: " + headerlessProducerRecord.value());
                System.out.println("Topic: " + headerlessProducerRecord.topic());

            }
        });
        System.out.println("Sent message: " + messageObject);
        kafkaProducer.flush();
    }

}
