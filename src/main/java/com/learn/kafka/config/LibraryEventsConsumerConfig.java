package com.learn.kafka.config;

import com.learn.kafka.exception.LibraryEventNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.InvalidPropertyException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;
import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dead-letter}")
    private String deadLetterTopic;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
        ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
        ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(consumerErrorHandler());
        return factory;
    }

    public DeadLetterPublishingRecoverer recoveryPublisher() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (r, ex) -> {
                if (ex.getCause() instanceof RecoverableDataAccessException) {
                    return new TopicPartition(retryTopic, r.partition());
                }
                if (ex.getCause() instanceof IllegalArgumentException) {
                    return new TopicPartition(deadLetterTopic, r.partition());
                }

                return new TopicPartition("todo-topic", r.partition());
            });
        return  recoverer;
    }

    public DefaultErrorHandler consumerErrorHandler() {
        var retryableExceptions = List.of(RecoverableDataAccessException.class);
        var exceptionsToIgnore = List.of(
            IllegalArgumentException.class,
            LibraryEventNotFoundException.class
        );
        var fixedBackOff = new FixedBackOff(1000L, 2);
        var exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1000L);
        exponentialBackOff.setMultiplier(2);
        exponentialBackOff.setMaxInterval(2000L);

        var errorHandler = new DefaultErrorHandler(recoveryPublisher(), fixedBackOff);

        exceptionsToIgnore.forEach(errorHandler::addNotRetryableExceptions);

        retryableExceptions.forEach(errorHandler::addRetryableExceptions);

        errorHandler.setRetryListeners((((record, ex, deliveryAttempt) -> {
            log.error("Failed Record in Retry Listener, Exception: {}", ex.getMessage());
            log.error("Failed Record in Retry Listener, Delivery Attempt: {}", deliveryAttempt);
        })));

        return errorHandler;
    }
}
