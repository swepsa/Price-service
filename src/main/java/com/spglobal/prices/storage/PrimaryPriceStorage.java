package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;

import java.util.List;
import java.util.Set;

/**
 * Thread-safe storage for the latest price records of financial instruments.
 * <p>
 * Supports atomic updates and snapshot reads. Consumers can request the latest prices
 * for a set of instrument IDs. Producers can update records in batches.
 */
public interface PrimaryPriceStorage {

    /**
     * Returns the latest price records for the given instrument IDs.
     * <p>
     * The returned maps are immutable snapshots, safe for concurrent access.
     *
     * @param ids the set of instrument IDs to fetch prices for
     * @return an immutable map where the key is the instrument ID and the value
     * is an immutable map representing the price payload
     */
    ImmutableMap<String, ImmutableMap<String, Object>> getLatest(Set<String> ids);

    /**
     * Updates the storage with the provided price records.
     * <p>
     * If multiple records exist for the same instrument ID, the one with the latest
     * {@code asOf} timestamp will be retained. The update is atomic and thread-safe.
     *
     * @param newRecords the list of price records to apply
     */
    void updateRecords(List<PriceData> newRecords);
}
