package com.amazon.rdsdata.client;

import com.amazon.rdsdata.client.testutil.TestBase;
import com.amazonaws.services.rdsdata.model.Field;
import lombok.val;
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SingleResultTests extends TestBase {
  @Test
  void shouldMapToSingleObject() {
    mockReturnValue(mockColumn("fieldName", new Field().withLongValue(1L)));

    val result = client.forSql("SELECT *")
        .execute()
        .singleValue(Long.class);

    assertThat(result).isEqualTo(1L);
  }

  @Test
  void shouldThrowExceptionIfResultSetHasNoRows() {
    mockReturnValues();

    assertThatThrownBy(() -> client.forSql("SELECT *").execute().singleValue(Long.class))
        .isInstanceOf(EmptyResultSetException.class);
  }

  @Test
  void shouldThrowExceptionIfResultSetHasNoColumns() {
    mockReturnValues(emptyList(), emptyList()); // add two empty rows

    assertThatThrownBy(() -> client.forSql("SELECT *").execute().singleValue(Long.class))
        .isInstanceOf(EmptyResultSetException.class);
  }
}
