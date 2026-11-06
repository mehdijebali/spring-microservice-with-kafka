package com.example.service;

import java.util.UUID;

import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.example.rest.CreateProductRestModel;

@Service
public class ProductServiceImpl implements ProductService {

    private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductRestModel product) throws Exception {

        String productId = UUID.randomUUID().toString(); // Generate a unique product ID
        // Implement the logic to create a product
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
            product.getTitle(),
            product.getPrice(),
            product.getQuantity());
        
        // Block until the broker acknowledges, so a failed send reaches the caller
        SendResult<String, ProductCreatedEvent> result = kafkaTemplate
            .send("product-created-events-topic", productCreatedEvent)
            .get();

        LOGGER.info("Partition:" + result.getRecordMetadata().partition());
        LOGGER.info("Offset:" + result.getRecordMetadata().offset());
        LOGGER.info("Topic:" + result.getRecordMetadata().topic());

        LOGGER.info("Product created event sent successfully for productId: {}", productId);
        
        return productId;
    }
}
