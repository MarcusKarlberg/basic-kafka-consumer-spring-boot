package se.marcuskarlberg.EmailNotificationService;

public class NotRetriableException extends RuntimeException {
  public NotRetriableException(String message) {
    super(message);
  }

  public NotRetriableException(Throwable cause) {
    super(cause);
  }
}
