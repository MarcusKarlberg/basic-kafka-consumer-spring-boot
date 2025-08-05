package se.marcuskarlberg.EmailNotificationService.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import se.marcuskarlberg.EmailNotificationService.entity.ProcessedEvent;

import java.util.Optional;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
  Optional<ProcessedEvent> findByMessageId(String messageId);
}
