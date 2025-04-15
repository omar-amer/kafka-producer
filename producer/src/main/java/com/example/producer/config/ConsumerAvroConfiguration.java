package com.example.producer.config;

import com.example.producer.model.Album;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class ConsumerAvroConfiguration {

    protected abstract boolean isActive();
    protected abstract String groupId();
    protected abstract String topic();

    @PostConstruct
    public void startReading() {
        if(isActive()) {
            buildAndSubscribe();
            new Thread(this::read).start();
        }
    }

    protected KafkaConsumer<Double, Album> consumer;

    protected void buildAndSubscribe() {
        Map<String, Object> consumerProperties = new HashMap<>();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProperties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
        consumerProperties.put("schema.registry.url", "http://localhost:8081");
        consumerProperties.put("specific.avro.reader","true");
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(topic()));
    }

    protected void read() {
        var name = this.getClass().getSimpleName();
        while (true) {
            ConsumerRecords<Double, Album> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Double, Album> record : records) {
                System.out.println(name + " message for partition: " + record.partition() + " ===> " + record.key() + ":" + record.value());
                record.headers().forEach(header ->
                        System.out.println(name + " message Headers: { " + header.key() + " : " + new String(header.value(), StandardCharsets.UTF_8) + " }")
                );
            }
            consumer.commitSync();
        }
    }
}
