package com.amazon.delivery.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.amazon.delivery.event.PaymentSuccessEvent;
import com.amazon.delivery.service.DeliveryService;

@Component
public class DeliveryConsumer {

    private final DeliveryService deliveryService;

    public DeliveryConsumer(DeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    @KafkaListener(
            topics = "payment-success",
            groupId = "delivery-service-group"
    )
    public void consumePaymentSuccess(PaymentSuccessEvent event) {

        System.out.println(
                "Received PAYMENT_SUCCESS event for orderId: "
                + event.getOrderId()
        );

        deliveryService.createDelivery(event);
    }
}