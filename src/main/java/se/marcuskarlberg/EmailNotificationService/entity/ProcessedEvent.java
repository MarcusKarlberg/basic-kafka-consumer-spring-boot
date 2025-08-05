package se.marcuskarlberg.EmailNotificationService.entity;

import jakarta.persistence.*;
import lombok.*;

import java.io.Serial;
import java.io.Serializable;

@Entity
@Table
@NoArgsConstructor
@Getter
@Setter
public class ProcessedEvent implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue
  private Long id;

  @Column(nullable = false, unique = true)
  private String messageId;

  @Column(nullable = false)
  private String productId;

  public ProcessedEvent(String messageId, String productId) {
    this.messageId = messageId;
    this.productId = productId;
  }
}
