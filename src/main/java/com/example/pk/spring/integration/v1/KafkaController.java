package com.example.pk.spring.integration.v1;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;

@RestController
public class KafkaController {
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private  TopicProducer topicProducer;
    @GetMapping(value = "/send")
    public void send(){
        topicProducer.send("This is a message to test if spring integration is working.");
    }
    @GetMapping(value = "/sendchannel")
    public void sendToChannel(){
        MessageChannel producingChannel =
                applicationContext.getBean("toKafka", MessageChannel.class);

        Map<String, Object> headers =
                Collections.singletonMap(KafkaHeaders.TOPIC, "TEST GEADERSGEADERSß");

        for (int i = 0; i < 10; i++) {
            GenericMessage<String> message =
                    new GenericMessage<>("Hello Spring Integration Kafka " + i + "!", headers);
            producingChannel.send(message);
        }
    }

}