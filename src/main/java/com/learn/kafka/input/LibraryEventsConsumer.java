package com.learn.kafka.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafka.exception.LibraryEventNotFoundException;
import com.learn.kafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {

    @Autowired
    LibraryEventService libraryEventService;

    @KafkaListener(topics = {"library.events"})
    public void onMessageReceived(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException, LibraryEventNotFoundException {
        log.info("Message received: {}", consumerRecord.value());
        libraryEventService.processEvent(consumerRecord);
    }
}
