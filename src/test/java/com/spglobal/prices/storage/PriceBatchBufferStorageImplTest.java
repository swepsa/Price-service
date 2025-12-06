package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.exception.NoSuchBatchException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PriceBatchBufferStorageImplTest {

    private PriceBatchBufferStorageImpl storage;

    @BeforeEach
    void setUp() {
        storage = new PriceBatchBufferStorageImpl();
    }

    @Test
    void testStartBatch() {
        String batchId = storage.startBatch();
        assertNotNull(batchId);
    }

    @Test
    void testAddChunk() {
        String batchId = storage.startBatch();
        PriceData price1 = new PriceData("A", Instant.now(), ImmutableMap.of("price", 100));
        PriceData price2 = new PriceData("B", Instant.now(), ImmutableMap.of("price", 200));

        storage.addChunk(batchId, List.of(price1, price2));

        ImmutableList<PriceData> result = storage.completeBatch(batchId);
        assertEquals(2, result.size());
        assertTrue(result.contains(price1));
        assertTrue(result.contains(price2));
    }

    @Test
    void testCompleteBatchThrowsIfNotExist() {
        String batchId = UUID.randomUUID().toString();
        assertThrows(NoSuchBatchException.class, () -> storage.completeBatch(batchId));
    }

    @Test
    void testCancelBatch_notAbleToAddBatch() {
        String batchId = storage.startBatch();
        storage.cancelBatch(batchId);

        List<PriceData> prices = List.of(new PriceData("A", Instant.now(), ImmutableMap.of("price", 100)));
        assertThrows(NoSuchBatchException.class, () -> storage.addChunk(batchId, prices));
    }

    @Test
    void testCancelBatch_notAbleToCancelTwoTimes() {
        String batchId = storage.startBatch();
        storage.cancelBatch(batchId);

        assertThrows(NoSuchBatchException.class, () -> storage.cancelBatch(batchId));
    }

    @Test
    void testCancelBatch_notAbleToCompleteAfterCancellation() {
        String batchId = storage.startBatch();
        storage.cancelBatch(batchId);

        assertThrows(NoSuchBatchException.class, () -> storage.completeBatch(batchId));
    }

    @Test
    void testAddChunkThrowsIfBatchDoesNotExist() {
        String batchId = UUID.randomUUID().toString();
        List<PriceData> prices = List.of(new PriceData("A", Instant.now(), ImmutableMap.of("price", 100)));

        assertThrows(NoSuchBatchException.class, () -> storage.addChunk(batchId, prices));
    }

    @Test
    void testAddChunkWithEmptyOrNullList() {
        String batchId = storage.startBatch();
        storage.addChunk(batchId, List.of());
        storage.addChunk(batchId, null);

        // Still able to complete batch (should be empty)
        ImmutableList<PriceData> result = storage.completeBatch(batchId);
        assertTrue(result.isEmpty());
    }

    @Test
    void testAddChunk_validateParameters_throwsOnNullBatchId() {
        List<PriceData> prices = List.of(new PriceData("A", Instant.now(), ImmutableMap.of("price", 100)));

        assertThrows(NullPointerException.class, () -> storage.addChunk(null, prices));
    }

    @Test
    void testCompleteBatch_throwsOnNullBatchId() {
        assertThrows(NullPointerException.class, () -> storage.completeBatch(null));
    }

    @Test
    void testCancelBatch_throwsOnNullBatchId() {
        assertThrows(NullPointerException.class, () -> storage.cancelBatch(null));
    }
}
