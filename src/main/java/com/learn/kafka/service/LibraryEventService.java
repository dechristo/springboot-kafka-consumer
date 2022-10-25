package com.learn.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.entity.LibraryEvent;
import com.learn.kafka.exception.LibraryEventNotFoundException;
import com.learn.kafka.repository.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    public void processEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException, LibraryEventNotFoundException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("LibraryEvent received: {}", libraryEvent.toString());

        switch (libraryEvent.getEventType()) {
            case CREATE -> save(libraryEvent);
            case UPDATE -> {
                validate(libraryEvent);
                save(libraryEvent);
            }
            default -> log.error("Library event type not supported: {}", libraryEvent.getEventType());
        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Library Event with id {} saved.", libraryEvent.getEventId());
    }

    private void validate(LibraryEvent libraryEvent) throws LibraryEventNotFoundException {
        if(libraryEvent.getEventId() == null) {
            log.error("LibraryEventId argument is mandatory.");
            throw new IllegalArgumentException("LibraryEventId mandatory argument is not present.");
        }

        if (libraryEvent.getEventId().equals(-99)) {
            log.warn("LibraryEventId used for integration tests.");
            throw new RecoverableDataAccessException("LibraryEventId used for integration tests.");
        }

        Optional<LibraryEvent> foundLibraryEvent = libraryEventsRepository.findById(libraryEvent.getEventId());
        if (foundLibraryEvent.isEmpty()) {
            log.error("LibraryEventId not found.");
            throw new LibraryEventNotFoundException("LibraryEventId not found.");
        }

        log.info("LibraryEvent with id {} is valid.", libraryEvent.getEventId());
    }
}
