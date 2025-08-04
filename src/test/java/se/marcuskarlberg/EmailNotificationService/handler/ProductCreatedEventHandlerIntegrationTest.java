package se.marcuskarlberg.EmailNotificationService.handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.*;
import se.marcuskarlberg.core.ProductCreatedEvent;

import java.math.BigDecimal;
import java.util.UUID;

import static se.marcuskarlberg.EmailNotificationService.handler.ProductCreatedEventHandler.TOPIC_NAME;

@EmbeddedKafka //To start embedded kafka server
@SpringBootTest(properties="spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreatedEventHandlerIntegrationTest {

  @Test
  void testProductCreatedEventHandler() {
    ProductCreatedEvent productCreatedEvent = ProductCreatedEvent.builder()
      .title("test title")
      .price(BigDecimal.ONE)
      .productId(UUID.randomUUID().toString())
      .quantity(1)
      .build();

    String messageId = UUID.randomUUID().toString();
    String messageKey = productCreatedEvent.getProductId();

    ProducerRecord<String, ProductCreatedEvent> record =
      new ProducerRecord<>(TOPIC_NAME, messageKey, productCreatedEvent);

    record.headers().add("messageId", messageId.getBytes());
    record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

    //TODO: act and assertions
  }
}
