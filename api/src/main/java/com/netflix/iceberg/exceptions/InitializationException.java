package com.netflix.iceberg.exceptions;

public class InitializationException extends RuntimeException {
  public InitializationException(String message, Object... args) {
    super(String.format(message, args));
  }

  public InitializationException(Throwable cause, String message, Object... args) {
    super(String.format(message, args), cause);
  }
}
