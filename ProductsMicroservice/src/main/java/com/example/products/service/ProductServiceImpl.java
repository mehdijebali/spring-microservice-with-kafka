package com.example.products.service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.example.products.rest.CreateProductRestModel;

@Service
public class ProductServiceImpl implements ProductService {

    private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductRestModel product) {

        String productId = UUID.randomUUID().toString(); // Generate a unique product ID
        // Implement the logic to create a product
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
            product.getTitle(),
            product.getPrice(),
            product.getQuantity());
        
            CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate.send("product-created-events-topic", productCreatedEvent);
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    // Handle the exception if the message sending fails
                    System.err.println("Failed to send product created event: " + ex.getMessage());
                } else {
                    // Message sent successfully
                    LOGGER.info("Partition:" + result.getRecordMetadata().partition());
                    LOGGER.info("Offset:" + result.getRecordMetadata().offset());
                    LOGGER.info("Topic:" + result.getRecordMetadata().topic());
                }
            });

        

        LOGGER.info("Product created event sent successfully for productId: {}", productId);
        
        return productId;
    }
}
