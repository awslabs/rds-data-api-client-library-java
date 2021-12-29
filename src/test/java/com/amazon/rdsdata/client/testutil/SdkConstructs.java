package com.amazon.rdsdata.client.testutil;

import com.amazonaws.services.rdsdata.model.Field;
import com.amazonaws.services.rdsdata.model.SqlParameter;

import java.nio.ByteBuffer;

public final class SdkConstructs {
  public static Field longField(long value) {
    return new Field().withLongValue(value);
  }

  public static Field doubleField(double value) {
    return new Field().withDoubleValue(value);
  }

  public static Field stringField(String value) {
    return new Field().withStringValue(value);
  }

  public static Field blobField(byte[] value) {
    return new Field().withBlobValue(ByteBuffer.wrap(value));
  }

  public static Field booleanField(boolean value) {
    return new Field().withBooleanValue(value);
  }

  public static Field nullField() {
    return new Field().withIsNull(true);
  }

  public static SqlParameter parameter(String name, Field value) {
    return new SqlParameter()
        .withName(name)
        .withValue(value);
  }

  public static SqlParameter parameter(String name, Field value, String hint) {
    return new SqlParameter()
        .withName(name)
        .withValue(value)
        .withTypeHint(hint);
  }
}
