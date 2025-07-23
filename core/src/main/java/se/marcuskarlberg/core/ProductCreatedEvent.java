package se.marcuskarlberg.core;

import lombok.*;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor  // Needed of deserialization
@Getter
@Setter
@Builder
public class ProductCreatedEvent {
  private String productId;
  private String title;
  private BigDecimal price;
  private Integer quantity;
}
