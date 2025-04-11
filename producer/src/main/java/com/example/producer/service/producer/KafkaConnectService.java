package com.example.producer.service.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Objects;
@Service
public class KafkaConnectService extends AbstractProducerService {
    private final KafkaProducer<Double, String> kafkaProducer;
    private final String TOPIC = "connect-log";

    public KafkaConnectService(@Qualifier("kafkaSinkProducer")
                                KafkaProducer<Double, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }
 @Scheduled(fixedDelay = 10000)
    public void sendMessage() {
      super.sendMessage();
    }

    @Override
    protected boolean isActive() {
        return false;
    }

    @Override
    protected void publish() {
        for (int i = 0; i < 10; i++) {
            double key = Math.floor(Math.random() * 50);
            String value = "Some logging info from kafka with key" + key;
            ProducerRecord<Double, String> producerRecord = new ProducerRecord<>(TOPIC, key, value);
            kafkaProducer.send(producerRecord, (metadata, e) -> {
                if (Objects.nonNull(metadata)) {
                    System.out.println("Key: " + producerRecord.key());
                    System.out.println("Value: " + producerRecord.value());
                    System.out.println("Topic: " + producerRecord.topic());
                    System.out.println("Partition: " + producerRecord.partition());
                }
            });
            System.out.println("Sent message: " + producerRecord);
        }
        kafkaProducer.flush();
    }
}
