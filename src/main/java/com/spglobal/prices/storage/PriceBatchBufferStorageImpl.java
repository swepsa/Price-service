package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableList;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.exception.NoSuchBatchException;

import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe in-memory implementation of {@link PriceBatchBufferStorage}.
 * <p>
 * Supports concurrent chunk uploads for the same batch. Once a batch is completed,
 * all records become visible atomically via an immutable snapshot.
 */
public class PriceBatchBufferStorageImpl implements PriceBatchBufferStorage {
    private final ConcurrentMap<String, ConcurrentLinkedQueue<PriceData>> storage = new ConcurrentHashMap<>();

    @Override
    public String startBatch() {
        String batchId = UUID.randomUUID().toString();
        storage.put(batchId, new ConcurrentLinkedQueue<>());
        return batchId;
    }

    @Override
    public void addChunk(String batchId, List<PriceData> chunk) {
        validateParameters(batchId);

        if (chunk == null || chunk.isEmpty()) {
            return;
        }

        Queue<PriceData> batch = storage.get(batchId);
        if (batch == null) {
            throw new NoSuchBatchException(batchId);
        }

        batch.addAll(chunk);
    }

    @Override
    public ImmutableList<PriceData> completeBatch(String batchId) {
        validateParameters(batchId);

        Queue<PriceData> batch = storage.remove(batchId);

        if (batch == null) {
            throw new NoSuchBatchException(batchId);
        }

        return ImmutableList.copyOf(batch);
    }

    @Override
    public void cancelBatch(String batchId) {
        validateParameters(batchId);

        if (storage.remove(batchId) == null) {
            throw new NoSuchBatchException(batchId);
        }
    }

    private void validateParameters(String batchId) {
        Objects.requireNonNull(batchId, "batchId cannot be null");
    }
}
