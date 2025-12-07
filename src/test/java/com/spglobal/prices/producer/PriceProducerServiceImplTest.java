package com.spglobal.prices.producer;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.exception.NoSuchBatchException;
import com.spglobal.prices.storage.PriceBatchBufferStorage;
import com.spglobal.prices.storage.PrimaryPriceStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PriceProducerServiceImplTest {

    private PriceBatchBufferStorage bufferStorage;
    private PrimaryPriceStorage primaryStorage;
    private PriceProducerServiceImpl service;

    @BeforeEach
    void setUp() {
        bufferStorage = mock(PriceBatchBufferStorage.class);
        primaryStorage = mock(PrimaryPriceStorage.class);
        service = new PriceProducerServiceImpl(bufferStorage, primaryStorage);
    }

    @Test
    void testStartBatch_delegatesToBuffer() {
        when(bufferStorage.startBatch()).thenReturn("batch-1");

        String result = service.startBatch();

        assertEquals("batch-1", result);
        verify(bufferStorage).startBatch();
    }

    @Test
    void testUploadChunk_delegatesToBuffer() {
        PriceData p = new PriceData("X", Instant.now(), ImmutableMap.of("v", 1));

        service.uploadChunk("b1", List.of(p));

        verify(bufferStorage).addChunk("b1", List.of(p));
    }

    @Test
    void testUploadChunk_withEmptyList_doesNotCallBuffer() {
        service.uploadChunk("b1", List.of());

        verify(bufferStorage, never()).addChunk(anyString(), any());
    }

    @Test
    void testUploadChunk_nullBatchIdThrows() {
        List<PriceData> prices = List.of(new PriceData("A", Instant.now(), ImmutableMap.of()));

        assertThrows(NullPointerException.class, () -> service.uploadChunk(null, prices));
    }

    @Test
    void testUploadChunk_nullChunkThrows() {
        assertThrows(NullPointerException.class, () -> service.uploadChunk("batch", null));
    }

    @Test
    void testUploadChunkPropagates_noSuchBatchException() {
        List<PriceData> prices = List.of(new PriceData("A", Instant.now(), ImmutableMap.of()));

        doThrow(new NoSuchBatchException("b1")).when(bufferStorage).addChunk(eq("b1"), any());

        assertThrows(NoSuchBatchException.class, () -> service.uploadChunk("b1", prices));
    }


    @Test
    void testCompleteBatch_success() {
        PriceData p = new PriceData("A", Instant.now(), ImmutableMap.of("x", 10));
        List<PriceData> batch = List.of(p);

        when(bufferStorage.completeBatch("b1")).thenReturn(batch);

        service.completeBatch("b1");

        // Ensure delegating to primary storage
        verify(primaryStorage).updateRecords(batch);
    }

    @Test
    void testCompleteBatch_withEmptyResult_doesNotUpdateStorage() {
        when(bufferStorage.completeBatch("b1")).thenReturn(List.of());

        service.completeBatch("b1");

        verify(primaryStorage, never()).updateRecords(any());
    }

    @Test
    void testCompleteBatch_nullThrows() {
        assertThrows(NullPointerException.class, () -> service.completeBatch(null));
    }

    @Test
    void testCompleteBatch_throwsNoSuchBatchException() {
        doThrow(new NoSuchBatchException("b1")).when(bufferStorage).completeBatch("b1");

        assertThrows(NoSuchBatchException.class, () -> service.completeBatch("b1"));
    }

    @Test
    void testCancelBatch_delegatesToBuffer() {
        service.cancelBatch("b1");
        verify(bufferStorage).cancelBatch("b1");
    }

    @Test
    void testCancelBatch_npeThrows() {
        assertThrows(NullPointerException.class, () -> service.cancelBatch(null));
    }

    @Test
    void testCancelBatch_throwsNoSuchBatchException() {
        doThrow(new NoSuchBatchException("b1")).when(bufferStorage).cancelBatch("b1");

        assertThrows(NoSuchBatchException.class, () -> service.cancelBatch("b1"));
    }
}
