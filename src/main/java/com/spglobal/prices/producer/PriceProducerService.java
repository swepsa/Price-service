package com.spglobal.prices.producer;

import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.exception.NoSuchBatchException;

import java.util.List;

/**
 * Service API for producing latest price values.
 * <p>
 * Producers are assumed to run in the same JVM.
 * This API supports concurrent chunk uploads for the same batch.
 */
public interface PriceProducerService {

    /**
     * Starts a new batch and returns its unique ID (UUID string).
     *
     * @return a new batch ID
     */
    String startBatch();


    /**
     * Uploads a chunk of price records for the given batch.
     * <p>
     * This method is thread-safe and can be called concurrently from multiple threads
     * for the same batch.
     *
     * @param batchId the ID of the batch to upload to
     * @param chunk   the list of price records to upload
     * @throws NoSuchBatchException if the batch with the given ID does not exist
     */
    void uploadChunk(String batchId, List<PriceData> chunk);


    /**
     * Completes the batch, making all its prices visible atomically to consumers.
     * <p>
     * After completion, producers cannot add new chunks to this batch.
     *
     * @param batchId the ID of the batch to complete
     * @throws NoSuchBatchException if the batch with the given ID does not exist
     */
    void completeBatch(String batchId);


    /**
     * Cancels the batch and discards all staged data.
     *
     * @param batchId the ID of the batch to cancel
     */
    void cancelBatch(String batchId) throws NoSuchBatchException;

}
