package com.example.pk.spring.integration.v1;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TopicProducer {

    private String topicName="myinput";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public TopicProducer(String topicName) {
        this.topicName = topicName;
    }

    public void send(String message){
        log.info("Payload enviado: {}", message);
        kafkaTemplate.send(topicName, message);
    }
}
