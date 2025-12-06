package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableList;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.exception.NoSuchBatchException;

import java.util.List;

/**
 * Storage for staging price records in batches before committing them to the main storage.
 * <p>
 * Supports concurrent chunk uploads for the same batch. Once a batch is completed,
 * all its records become visible atomically to consumers.
 */
public interface PriceBatchBufferStorage {

    /**
     * Starts a new batch and returns its unique batch ID.
     *
     * @return the ID of the newly created batch
     */
    String startBatch();

    /**
     * Adds a chunk of price records to the specified batch.
     * <p>
     * This method is thread-safe and can be called concurrently for the same batch.
     *
     * @param batchId the ID of the batch to add records to
     * @param chunk   the list of price records to add
     * @throws NoSuchBatchException if the batch with the given ID does not exist,
     * has been cancelled, or has already been completed
     */
    void addChunk(String batchId, List<PriceData> chunk);

    /**
     * Completes the batch, making all its records visible atomically to consumers.
     * <p>
     * After completion, no more chunks can be added to this batch.
     *
     * @param batchId the ID of the batch to complete
     * @return an immutable list of all price records in this batch
     * @throws NoSuchBatchException if the batch with the given ID does not exist,
     * has been cancelled, or has already been completed
     */
    ImmutableList<PriceData> completeBatch(String batchId);

    /**
     * Cancels the batch, discarding all staged data.
     * <p>
     * After cancellation, the batch is no longer accessible.
     *
     * @param batchId the ID of the batch to cancel
     * @throws NoSuchBatchException if the batch with the given ID does not exist,
     * has been cancelled, or has already been completed
     */
    void cancelBatch(String batchId);
}
