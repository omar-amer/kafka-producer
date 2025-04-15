package com.example.producer.service.producer;

import com.example.producer.model.Album;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Random;

import static com.example.producer.avro.Constants.CONNECT_DISTRIBUTED;
import static com.example.producer.avro.Constants.TOPIC;

@Service
public class KafkaAvroService extends AbstractProducerService{

  private final KafkaProducer<Double, Album> kafkaProducer;

    public KafkaAvroService(@Qualifier("kafkaAvroProducer") KafkaProducer<Double, Album> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Scheduled(fixedDelay = 10000)
    public void sendMessage() {
      super.sendMessage();
    }

    @Override
    protected boolean isActive() {
        return true;
    }

    @Override
    protected void publish() {
        for (int i = 0; i < 10; i++) {
            double key = Math.floor(Math.random() * 50);
            Album  value= new Album("Use Your Illusion",new Random().nextInt(1950,2000 + 1950));
            ProducerRecord<Double, Album> producerRecord = new ProducerRecord<>(CONNECT_DISTRIBUTED, key, value);
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
