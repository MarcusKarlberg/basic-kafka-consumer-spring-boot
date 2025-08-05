package se.marcuskarlberg.EmailNotificationService.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.*;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import se.marcuskarlberg.EmailNotificationService.entity.ProcessedEvent;
import se.marcuskarlberg.EmailNotificationService.exception.NotRetriableException;
import se.marcuskarlberg.EmailNotificationService.exception.RetriableException;
import se.marcuskarlberg.EmailNotificationService.repository.ProcessedEventRepository;
import se.marcuskarlberg.core.ProductCreatedEvent;

import java.util.Optional;

import static se.marcuskarlberg.EmailNotificationService.handler.ProductCreatedEventHandler.TOPIC_NAME;

@Component
@KafkaListener(topics = TOPIC_NAME)
public class ProductCreatedEventHandler {
  private final static Logger LOG = LoggerFactory.getLogger(ProductCreatedEventHandler.class);
  final static String TOPIC_NAME = "product-created-events-topic";

  public RestTemplate restTemplate;

  @Autowired
  private ProcessedEventRepository processedEventRepository;

  ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
    this.restTemplate = restTemplate;
    this.processedEventRepository = processedEventRepository;
  }

  @Transactional //Handles db rollback on any exceptions
  @KafkaHandler
  public void handler(@Payload ProductCreatedEvent productCreateEvent,
                      @Header("messageId") String messageId,
                      @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {

    String requestUrl = "http://localhost:8080";

    LOG.info(
      "NEW EVENT:  {} with productId: {}", productCreateEvent.getTitle(), productCreateEvent.getProductId()
    );

    Optional<ProcessedEvent> existingRecord = processedEventRepository.findByMessageId(messageId);
    if(existingRecord.isPresent()) {
      LOG.info("Duplicate messageId found: {}", messageId);
      return;
    }

    try {
      ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);
      if(response.getStatusCode().value() == HttpStatus.OK.value()) {
        LOG.info("Event data: Title: {} - Price: {} - Quantity: {} - Id: {}",
          productCreateEvent.getTitle(), productCreateEvent.getPrice(), productCreateEvent.getQuantity(), productCreateEvent.getProductId());
      }
    } catch (ResourceAccessException e) {
      LOG.error(e.getMessage());
      throw new RetriableException(e.getMessage());
    } catch (Exception e) {     //Other exceptions will not be retried.
      LOG.error(e.getMessage());
      throw new NotRetriableException(e.getMessage());
    }

    try {
      processedEventRepository.save(new ProcessedEvent(messageId, messageKey));
    } catch (DataIntegrityViolationException e) {
      throw new NotRetriableException(e.getMessage());
    }
  }
}
