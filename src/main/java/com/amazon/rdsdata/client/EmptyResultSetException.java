package com.amazon.rdsdata.client;

public class EmptyResultSetException extends RuntimeException {
  public EmptyResultSetException() {
    super("Result set is empty");
  }
}
