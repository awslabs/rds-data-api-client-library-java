package com.amazon.rdsdata.client;

import com.amazon.rdsdata.client.testutil.TestBase;
import lombok.Value;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RetrieveNumberOfRecordsTests extends TestBase {

    @Test
    void shouldReturnUpdatedRecordsCount() {
        val numberOfRecordsUpdated = 1;
        mockReturnValue(numberOfRecordsUpdated);

        val result = client.forSql("INSERT INTO tbl1(a, b, c) VALUES(?, ?, ?)", 1, 2, 3)
                .execute()
                .getNumberOfRecordsUpdated();

        assertThat(result).isEqualTo(numberOfRecordsUpdated);
    }

    @Test
    void shouldReturnZeroUpdatedRecordsCountForBatchUpdate() {
        mockReturnValues();

        val dto = new Dto(1);

        val result = client.forSql("INSERT INTO tbl1(a) VALUES(:value)")
                .withParamSets(dto)
                .execute()
                .getNumberOfRecordsUpdated();

        assertThat(result).isEqualTo(0);
    }

    @Value
    private static class Dto {
        public final int value;
    }
}