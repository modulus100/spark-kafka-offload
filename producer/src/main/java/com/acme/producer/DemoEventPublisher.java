package com.acme.producer;

import com.acme.demo.v1.DemoEvent;
import java.time.Instant;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DemoEventPublisher {

    private final KafkaTemplate<String, DemoEvent> kafkaTemplate;
    private final String topic;

    public DemoEventPublisher(
            KafkaTemplate<String, DemoEvent> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "5000")
    public void publish() {
        String id = UUID.randomUUID().toString();

        DemoEvent event = DemoEvent.newBuilder()
                .setId(id)
                .setCreatedAtEpochMs(Instant.now().toEpochMilli())
                .setPayload("hello from spring")
                .build();

        kafkaTemplate.send(topic, id, event);
    }
}
