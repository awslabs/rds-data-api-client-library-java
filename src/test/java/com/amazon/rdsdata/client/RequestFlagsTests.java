package com.amazon.rdsdata.client;

import com.amazon.rdsdata.client.testutil.TestBase;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RequestFlagsTests extends TestBase {
  @BeforeEach
  public void beforeEach() {
    mockReturnValue(); // return empty response by default
  }

  @Test
  public void shouldSetContinueAfterTimeoutFlag() {
    client.forSql("SELECT 1")
        .withContinueAfterTimeout()
        .execute();

    val request = captureRequest();
    assertThat(request.continueAfterTimeout()).isTrue();
  }

  @Test
  public void shouldNotSetContinueAfterTimeoutFlagByDefault() {
    client.forSql("SELECT 1")
        .execute();

    val request = captureRequest();
    assertThat(request.continueAfterTimeout()).isFalse();
  }
}
