package com.amazon.delivery.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.amazon.delivery.entity.DeliveryEntity;

public interface DeliveryRepository extends JpaRepository<DeliveryEntity, Long> {

}