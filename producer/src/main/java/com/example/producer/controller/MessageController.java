package com.example.producer.controller;

import com.example.producer.service.producer.KafkaProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class MessageController {
    private final KafkaProducerService producerService;

    public MessageController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestParam String topic,
                              @RequestParam String key,
                              @RequestParam String value) {

        return "Message sent successfully";
    }
}
