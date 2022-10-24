package com.learn.kafka.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.entity.Book;
import com.learn.kafka.entity.LibraryEvent;
import com.learn.kafka.entity.LibraryEventType;
import com.learn.kafka.exception.LibraryEventNotFoundException;
import com.learn.kafka.repository.LibraryEventsRepository;
import com.learn.kafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(topics = {"library.events"}, partitions = 3)
@TestPropertySource(properties = {
    "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
})
@Execution(ExecutionMode.SAME_THREAD)
public class LibraryEventConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventService libraryEventServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer: endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown(){
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishLibraryEventCreateBookIsSuccessful() throws ExecutionException, InterruptedException, LibraryEventNotFoundException, JsonProcessingException {
        String message =
            "{\"eventId\":null,\"eventType\":\"CREATE\",\"book\":{\"id\":11,\"title\":\"Night Prey\",\"author\":\"John Sandford\"}}";

        kafkaTemplate.sendDefault(message).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessageReceived(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();

        assertEquals(1, libraryEvents.size());
        assertEquals(11, libraryEvents.get(0).getBook().getId());
        assertEquals("Night Prey", libraryEvents.get(0).getBook().getTitle());
        assertEquals("John Sandford", libraryEvents.get(0).getBook().getAuthor());
    }

    @Test
    void publishLibraryEventUpdateBookIsSuccessful() throws ExecutionException, InterruptedException, LibraryEventNotFoundException, JsonProcessingException {
        String message =
            "{\"eventId\":null,\"eventType\":\"CREATE\",\"book\":{\"id\":20,\"title\":\"Ocean Prey\",\"author\":\"John Sandford\"}}";

        LibraryEvent libraryEvent = objectMapper.readValue(message, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder()
            .id(20)
            .title("Ocean Prey (Hardback)")
            .author("John Sandford")
            .build();

        libraryEvent.setEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);

        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(libraryEvent.getEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessageReceived(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getEventId()).get();
        assertEquals("Ocean Prey (Hardback)", persistedLibraryEvent.getBook().getTitle());
    }

    @Test
    void publishLibraryEventUpdateBookFailsIfEventIdIsNull() throws InterruptedException, LibraryEventNotFoundException, JsonProcessingException {
        String message =
            "{\"eventId\":null,\"eventType\":\"UPDATE\",\"book\":{\"id\":22,\"title\":\"Ocean Prey (paperback)\",\"author\":\"John Sandford\"}}";

        kafkaTemplate.sendDefault(message);

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(10)).onMessageReceived(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(10)).processEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishLibraryEventUpdateBookFailsIfEventIdDoesNotExists() throws InterruptedException, LibraryEventNotFoundException, JsonProcessingException {
        String message =
            "{\"eventId\":87655,\"eventType\":\"UPDATE\",\"book\":{\"id\":23,\"title\":\"Ocean Prey\",\"author\":\"John Sandford\"}}";

        kafkaTemplate.sendDefault(message);

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(10)).onMessageReceived(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(10)).processEvent(isA(ConsumerRecord.class));
    }
}
