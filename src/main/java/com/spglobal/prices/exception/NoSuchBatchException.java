package com.spglobal.prices.exception;

/**
 * Exception thrown when a batch with the given ID does not exist.
 */
public class NoSuchBatchException extends RuntimeException {

    /**
     * Constructs a new exception with a detailed message.
     *
     * @param batchId the ID of the batch that was not found
     */
    public NoSuchBatchException(String batchId) {
        super("No such batch: " + batchId);
    }
}