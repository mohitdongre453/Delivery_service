package com.amazon.delivery.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;

import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import org.springframework.util.backoff.FixedBackOff;

import com.amazon.delivery.event.PaymentSuccessEvent;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, PaymentSuccessEvent> consumerFactory() {

        Map<String, Object> properties = new HashMap<>();

        // Kafka server
        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        // Consumer group
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "delivery-service-group"
        );

        // Read old messages if no offset exists
        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        // =========================
        // KEY DESERIALIZER
        // =========================

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        properties.put(
                ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS,
                StringDeserializer.class
        );

        // =========================
        // VALUE DESERIALIZER
        // =========================

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        properties.put(
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JsonDeserializer.class
        );

        // =========================
        // TRUSTED PACKAGES
        // =========================

        properties.put(
                JsonDeserializer.TRUSTED_PACKAGES,
                "com.amazon.payment.event.*,com.amazon.delivery.event.*"
        );

        // =========================
        // TYPE MAPPING
        // =========================

        properties.put(
                JsonDeserializer.TYPE_MAPPINGS,
                "com.amazon.payment.event.PaymentSuccessEvent:com.amazon.delivery.event.PaymentSuccessEvent"
        );

        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentSuccessEvent>
            kafkaListenerContainerFactory(
                    ConsumerFactory<String, PaymentSuccessEvent> consumerFactory,
                    KafkaTemplate<String, Object> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, PaymentSuccessEvent>
                factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // =========================
        // DLT
        // =========================

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(
                                        record.topic() + ".DLT",
                                        record.partition()
                                )
                );

        // Retry 3 times
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(
                        recoverer,
                        new FixedBackOff(2000L, 3L)
                );

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}