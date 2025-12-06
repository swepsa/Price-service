package com.spglobal.prices.producer;

import com.google.common.collect.ImmutableList;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.exception.NoSuchBatchException;
import com.spglobal.prices.storage.PriceBatchBufferStorage;
import com.spglobal.prices.storage.PrimaryPriceStorage;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of {@link PriceProducerService}.
 * <p>
 * Supports concurrent chunk uploads for the same batch and ensures atomic batch completion.
 */
public class PriceProducerServiceImpl implements PriceProducerService {

    private final PriceBatchBufferStorage bufferStorage;
    private final PrimaryPriceStorage storage;

    public PriceProducerServiceImpl(PriceBatchBufferStorage bufferStorage, PrimaryPriceStorage storage) {
        this.bufferStorage = Objects.requireNonNull(bufferStorage, "bufferStorage cannot be null");
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
    }

    @Override
    public String startBatch() {
        return bufferStorage.startBatch();
    }

    @Override
    public void uploadChunk(String batchId, List<PriceData> chunk) throws NoSuchBatchException {
        validateParameters(batchId, chunk);

        if (chunk.isEmpty()) {
            return;
        }
        bufferStorage.addChunk(batchId, chunk);
    }

    @Override
    public void completeBatch(String batchId) throws NoSuchBatchException {
        validateParameters(batchId);
        ImmutableList<PriceData> batch = bufferStorage.completeBatch(batchId);

        if (batch == null || batch.isEmpty()) {
            return;
        }

        storage.updateRecords(batch);
    }

    @Override
    public void cancelBatch(String batchId) throws NoSuchBatchException {
        validateParameters(batchId);
        bufferStorage.cancelBatch(batchId);
    }

    private void validateParameters(String batchId, List<PriceData> chunk) {
        validateParameters(batchId);
        Objects.requireNonNull(chunk, "chunk cannot be null");
    }

    private void validateParameters(String batchId) {
        Objects.requireNonNull(batchId, "batchId cannot be null");
    }

}
