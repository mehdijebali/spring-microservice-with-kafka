package com.appsdeveloperblog.ws.emailnotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private final RestTemplate restTemplate;
    private final ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
        this.restTemplate = restTemplate;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional 
    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent productCreatedEvent, @Header ("messageId") String messageId,
                        @Header (KafkaHeaders.RECEIVED_KEY) String messageKey) {
        // Implement the logic to handle the product created event
        LOGGER.info("Received a new event: {}", productCreatedEvent.getTitle()+ " with productId: "
				+ productCreatedEvent.getProductId());

        // Check if this message was already processed before
		ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);

        if(existingRecord!=null) {
			LOGGER.info("Found a duplicate message id: {}", existingRecord.getMessageId());
			return;
		}

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

        // Save a unique message id in a database table
		try {
			processedEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));
		} catch (DataIntegrityViolationException ex) {
			throw new NotRetrayableException(ex);
		}
        
    }
}
