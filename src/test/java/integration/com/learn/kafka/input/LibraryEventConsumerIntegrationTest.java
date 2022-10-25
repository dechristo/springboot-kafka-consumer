package com.learn.kafka.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.entity.Book;
import com.learn.kafka.entity.LibraryEvent;
import com.learn.kafka.entity.LibraryEventType;
import com.learn.kafka.exception.LibraryEventNotFoundException;
import com.learn.kafka.repository.LibraryEventsRepository;
import com.learn.kafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(topics = {"library.events", "library.events.retry", "library.events.dead-letter"}, partitions = 3)
@TestPropertySource(properties = {
    "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "retryListener.startup=false"})
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

    private Consumer<Integer, String> consumer;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dead-letter}")
    private String deadLetterTopic;

    @BeforeEach
    void setUp() {
//        for (MessageListenerContainer messageListenerContainer: endpointRegistry.getListenerContainers()) {
//            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
//        }
        var container = endpointRegistry.getListenerContainers()
            .stream().filter(messageListenerContainer ->
                Objects.equals(messageListenerContainer.getGroupId(), "library-events-listener-group"))
            .collect(Collectors.toList()).get(0);
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
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
    void publishLibraryEventUpdateBookFailsIfEventIdDoesNotExists() throws InterruptedException, LibraryEventNotFoundException, JsonProcessingException {
        String message =
            "{\"eventId\":87655,\"eventType\":\"UPDATE\",\"book\":{\"id\":23,\"title\":\"Ocean Prey\",\"author\":\"John Sandford\"}}";

        kafkaTemplate.sendDefault(message);

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessageReceived(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishUpdateLibraryEventToRetryTopic() throws JsonProcessingException, ExecutionException, InterruptedException, LibraryEventNotFoundException {
        String json = "{\"eventId\":-99,\"eventType\":\"UPDATE\",\"book\":{\"id\":30,\"title\":\"Holy Ghost\",\"author\":\"John Sandford\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(3)).onMessageReceived(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(3)).processEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
        System.out.println("consumerRecord is : "+ consumerRecord.value());
        assertEquals(json, consumerRecord.value());
    }

    @Test
    void publishUpdateLibraryEventToDeadLetterQueue() throws JsonProcessingException, ExecutionException, InterruptedException, LibraryEventNotFoundException {
        String json = "{\"eventId\":null,\"eventType\":\"UPDATE\",\"book\":{\"id\":40,\"title\":\"Hellburner\",\"author\":\"Clive Cussler\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessageReceived(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, deadLetterTopic);
        System.out.println("consumerRecord is : "+ consumerRecord.value());
        assertEquals(json, consumerRecord.value());
    }
}
