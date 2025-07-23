package se.marcuskarlberg.EmailNotificationService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.*;
import org.springframework.stereotype.Component;
import se.marcuskarlberg.core.ProductCreatedEvent;

import static se.marcuskarlberg.EmailNotificationService.ProductCreatedEventHandler.TOPIC_NAME;

@Component
@KafkaListener(topics = TOPIC_NAME)
public class ProductCreatedEventHandler {
  private final static Logger LOG = LoggerFactory.getLogger(ProductCreatedEventHandler.class);
  final static String TOPIC_NAME = "product-created-events-topic";

  @KafkaHandler
  public void handler(ProductCreatedEvent productCreateEvent) {
    LOG.info("Received new event: Title: {} - Price: {} - Quantity: {} - Id: {}",
      productCreateEvent.getTitle(), productCreateEvent.getPrice(), productCreateEvent.getQuantity(), productCreateEvent.getProductId());
  }
}
