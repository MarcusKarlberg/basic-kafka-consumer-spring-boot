package se.marcuskarlberg.EmailNotificationService;

public class RetriableException extends RuntimeException{
  public RetriableException(String message) {
    super(message);
  }
  public RetriableException(String message, Throwable cause) {}
}
