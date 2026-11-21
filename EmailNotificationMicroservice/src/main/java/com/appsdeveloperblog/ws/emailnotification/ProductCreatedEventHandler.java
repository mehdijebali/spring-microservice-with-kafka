package com.appsdeveloperblog.ws.emailnotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private RestTemplate restTemplate;

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    public void handleProductCreatedEvent(ProductCreatedEvent productCreatedEvent) {
        // Implement the logic to handle the product created event
        LOGGER.info("Received a new event: {}", productCreatedEvent.getTitle());
        String requestUrl = "http://localhost:8082/response/200";

        try {
            ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);
        } catch (ResourceAccessException ex) {
            LOGGER.error("Error occurred while making the request: {}", ex.getMessage());
            throw new RetryableException("Error occurred while making the request: " + ex.getMessage());
        } catch (HttpServerErrorException ex) {
            LOGGER.error("An unexpected error occurred: {}", ex.getMessage());
            throw new NotRetrayableException("An unexpected error occurred: " + ex.getMessage());
        } catch (Exception ex) {
            LOGGER.error("An unexpected error occurred: {}", ex.getMessage());
            throw new NotRetrayableException("An unexpected error occurred: " + ex.getMessage());
        }
    }
}
