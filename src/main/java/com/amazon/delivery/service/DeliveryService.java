package com.amazon.delivery.service;

import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.amazon.delivery.entity.DeliveryEntity;
import com.amazon.delivery.event.DeliveryCreatedEvent;
import com.amazon.delivery.event.PaymentSuccessEvent;
import com.amazon.delivery.repository.DeliveryRepository;

@Service
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public DeliveryService(
            DeliveryRepository deliveryRepository,
            KafkaTemplate<String, Object> kafkaTemplate) {

        this.deliveryRepository = deliveryRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void createDelivery(PaymentSuccessEvent event) {

        System.out.println(
                "Creating delivery for orderId: "
                + event.getOrderId()
        );

        // 1. Generate tracking number
        String trackingNumber =
                "TRK-" + UUID.randomUUID();

        // 2. Create Delivery Entity
        DeliveryEntity delivery = new DeliveryEntity();

        delivery.setOrderId(event.getOrderId());
        delivery.setCustomerId(event.getCustomerId());
        delivery.setDeliveryAddress("Bangalore");
        delivery.setTrackingNumber(trackingNumber);
        delivery.setDeliveryStatus("CREATED");

        // 3. Save delivery into database
        DeliveryEntity savedDelivery =
                deliveryRepository.save(delivery);

        System.out.println(
                "Delivery created for orderId: "
                + savedDelivery.getOrderId()
        );

        System.out.println(
                "Tracking Number: "
                + savedDelivery.getTrackingNumber()
        );

        // 4. Create DELIVERY_CREATED event
        DeliveryCreatedEvent deliveryEvent =
                new DeliveryCreatedEvent();

        deliveryEvent.setEventId(
                "DEL-" + savedDelivery.getId()
        );

        deliveryEvent.setEventType(
                "DELIVERY_CREATED"
        );

        deliveryEvent.setOrderId(
                savedDelivery.getOrderId()
        );

        deliveryEvent.setCustomerId(
                savedDelivery.getCustomerId()
        );

        deliveryEvent.setTrackingNumber(
                savedDelivery.getTrackingNumber()
        );

        deliveryEvent.setDeliveryStatus(
                savedDelivery.getDeliveryStatus()
        );

        // 5. Publish event
        kafkaTemplate.send(
                "delivery-created",
                String.valueOf(savedDelivery.getOrderId()),
                deliveryEvent
        );

        System.out.println(
                "DELIVERY_CREATED event published for orderId: "
                + savedDelivery.getOrderId()
        );
    }
}