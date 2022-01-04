package com.amazon.rdsdata.client.testutil;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.rdsdata.model.Field;
import software.amazon.awssdk.services.rdsdata.model.SqlParameter;

public final class SdkConstructs {
  public static Field longField(long value) {
    return Field.builder().longValue(value).build();
  }

  public static Field doubleField(double value) {
    return Field.builder().doubleValue(value).build();
  }

  public static Field stringField(String value) {
    return Field.builder().stringValue(value).build();
  }

  public static Field blobField(byte[] value) {
    return Field.builder().blobValue(SdkBytes.fromByteArray(value)).build();
  }

  public static Field booleanField(boolean value) {
    return Field.builder().booleanValue(value).build();
  }

  public static Field nullField() {
    return Field.builder().isNull(true).build();
  }

  public static SqlParameter parameter(String name, Field value) {
    return SqlParameter.builder()
        .name(name)
        .value(value)
        .build();
  }

  public static SqlParameter parameter(String name, Field value, String hint) {
    return SqlParameter.builder()
        .name(name)
        .value(value)
        .typeHint(hint)
        .build();
  }
}
