package se.marcuskarlberg.EmailNotificationService.handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.*;
import org.springframework.web.client.RestTemplate;
import se.marcuskarlberg.EmailNotificationService.entity.ProcessedEvent;
import se.marcuskarlberg.EmailNotificationService.repository.ProcessedEventRepository;
import se.marcuskarlberg.core.ProductCreatedEvent;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static se.marcuskarlberg.EmailNotificationService.handler.ProductCreatedEventHandler.TOPIC_NAME;

@EmbeddedKafka //To start embedded kafka server
@SpringBootTest(properties="spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreatedEventHandlerIntegrationTest {

  @MockBean
  ProcessedEventRepository processedEventRepository;

  @MockBean
  RestTemplate restTemplate;

  @Autowired
  KafkaTemplate<String, Object> kafkaTemplate;

  @SpyBean
  ProductCreatedEventHandler eventHandler;

  @Autowired
  private ProductCreatedEventHandler productCreatedEventHandler;

  @Test
  void testProductCreatedEventHandler() throws ExecutionException, InterruptedException {
    ProductCreatedEvent productCreatedEvent = ProductCreatedEvent.builder()
      .title("test title")
      .price(BigDecimal.ONE)
      .productId(UUID.randomUUID().toString())
      .quantity(1)
      .build();

    String messageId = UUID.randomUUID().toString();
    String messageKey = productCreatedEvent.getProductId();

    ProducerRecord<String, Object> record =
      new ProducerRecord<>(TOPIC_NAME, messageKey, productCreatedEvent);

    record.headers().add("messageId", messageId.getBytes());
    record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

    when(processedEventRepository.findByMessageId(anyString()))
      .thenReturn(Optional.empty());
    when(processedEventRepository.save(any())).thenReturn(null);

    String responseBody = "{\"key\":\"value\"}";
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);

    when(restTemplate.exchange(any(String.class), any(HttpMethod.class), isNull(), eq(String.class)))
      .thenReturn(responseEntity);

    kafkaTemplate.send(record).get();

    ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

    verify(productCreatedEventHandler, timeout(5000).times(1)).handler(eventCaptor.capture(),
      messageIdCaptor.capture(),
      messageKeyCaptor.capture());

    assertEquals(messageId, messageIdCaptor.getValue());
    assertEquals(messageKey, messageKeyCaptor.getValue());
    assertEquals(productCreatedEvent.getProductId(), eventCaptor.getValue().getProductId());
  }
}
