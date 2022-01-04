package com.amazon.rdsdata.client;

import com.amazon.rdsdata.client.testutil.SdkConstructs;
import com.amazon.rdsdata.client.testutil.TestBase;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SingleParameterWitherTests extends TestBase {
  @BeforeEach
  public void beforeEach() {
    mockReturnValue(); // return empty response by default
  }

  @Test
  void shouldSupportWithParameter() {
    client.forSql("INSERT INTO table(a, b) VALUES(:a, :b)")
        .withParameter("a", 100)
        .withParameter("b", "hello")
        .execute();

    val request = captureRequest();
    assertThat(request.parameters()).containsExactly(
        SdkConstructs.parameter("a", SdkConstructs.longField(100L)),
        SdkConstructs.parameter("b", SdkConstructs.stringField("hello"))
    );
  }

  @Test
  void shouldNotSupportWithParameterAfterOtherOperations() {
    assertThatThrownBy(() -> {
      client.forSql("INSERT INTO table(a, b) VALUES(:a, :b)")
          .withParamSets(new Object(), new Object())
          .withParameter("a", 100);
    })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Parameters are already supplied");
  }
}
