package com.amazon.rdsdata.client;

import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import com.amazon.rdsdata.client.testutil.TestBase;
import com.amazonaws.services.rdsdata.model.Field;
import com.google.common.collect.ImmutableList;
import lombok.Value;
import lombok.val;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

public class RetrieveNumberOfRecordsTests extends TestBase {

    @Test
    void shouldReturnUpdatedRecordsCountForParameterUpdate() {
        long numberOfRecordsUpdated = 1;
        mockReturnValue(numberOfRecordsUpdated);

        val result = client.forSql("INSERT INTO tbl1(a, b, c) VALUES(?, ?, ?)", 1, 2, 3)
                .execute();
        assertEquals(numberOfRecordsUpdated, result.getNumberOfRecordsUpdated());
    }

    @Test
    void shouldReturnUpdatedRecordsCountForDtoParamSingleUpdate() {
        long numberOfRecordsUpdated = 1;
        mockReturnValue(numberOfRecordsUpdated);

        val dto = new Dto(1);

        val result = client.forSql("INSERT INTO tbl1(a) VALUES(:value)")
                .withParamSets(dto)
                .execute();
        assertEquals(numberOfRecordsUpdated, result.getNumberOfRecordsUpdated());
    }

    @Test
    void shouldReturnZeroUpdatedRecordsCountForNoUpdates() {
        mockReturnValue();

        val dto = new Dto(1);

        val result = client.forSql("INSERT INTO tbl1(a) VALUES(:value)")
                .withParamSets(dto)
                .execute();
        assertEquals(0, result.getNumberOfRecordsUpdated());
    }

    @Test
    void shouldReturnUpdatedRecordsCountForDtoParamBatchUpdate() {
        mockReturnValues(
                ImmutableList.of( // first row
                        mockColumn("intField", new Field().withLongValue(1L))
                ), ImmutableList.of( // 2nd row
                        mockColumn("intField", new Field().withLongValue(2L))
                ), ImmutableList.of( // 3rd row
                        mockColumn("intField", new Field().withLongValue(3L))
                ));

        val dto1 = new Dto(1);
        val dto2 = new Dto(2);
        val dto3 = new Dto(3);

        val result = client.forSql("INSERT INTO tbl1(a) VALUES(:value)")
                .withParamSets(dto1, dto2, dto3)
                .execute();
        assertEquals(0, result.getNumberOfRecordsUpdated());

    }

    @Value
    private static class Dto {
        public final int value;
    }
}