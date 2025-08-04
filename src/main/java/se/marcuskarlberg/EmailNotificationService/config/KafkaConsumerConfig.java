package se.marcuskarlberg.EmailNotificationService.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import se.marcuskarlberg.EmailNotificationService.exception.NotRetriableException;
import se.marcuskarlberg.EmailNotificationService.exception.RetriableException;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

  private final Environment environment;

  public KafkaConsumerConfig(Environment environment) {
    this.environment = environment;
  }

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    //ErrorHandlingDeserializer to handle errors gracefully.
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class); //Catch any deserializer exceptions
    config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class); //The actual deserialization should be done using JsonDeserializer

    config.put(JsonDeserializer.TRUSTED_PACKAGES, environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"));
    config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("consumer.group-id"));
    return new DefaultKafkaConsumerFactory<>(config);
  }

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
    ConsumerFactory<String, String> consumerFactory, KafkaTemplate<String, String> kafkaTemplate) {
    //Send errors to Dead Letter Topic - 3 retries, 5 s interval.
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
      new DeadLetterPublishingRecoverer(kafkaTemplate),
      new FixedBackOff(5000,3)
    );
    errorHandler.addRetryableExceptions(RetriableException.class);
    errorHandler.addNotRetryableExceptions(NotRetriableException.class);

    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(errorHandler);
    return factory;
  }

  //This is used to errors to Dead Letter Topics
  @Bean
  KafkaTemplate<String, String> createKafkaTemplate(ProducerFactory producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  //This is used to errors to Dead Letter Topics
  @Bean
  ProducerFactory<String, String> producerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return new DefaultKafkaProducerFactory<>(config);
  }
}
