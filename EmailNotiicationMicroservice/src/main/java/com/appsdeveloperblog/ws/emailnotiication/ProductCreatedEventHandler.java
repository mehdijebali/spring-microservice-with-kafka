package main.java.com.appsdeveloperblog.ws.emailnotiication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @KafkaHandler
    public void handleProductCreatedEvent(ProductCreatedEvent productCreatedEvent) {
        // Implement the logic to handle the product created event
        LOGGER.info("Received a new event: {}", productCreatedEvent.getTitle());
    }
}
