package com.spglobal.prices.consumer;

import com.google.common.collect.ImmutableMap;

import java.util.Set;

/**
 * Service API for consuming latest price values.
 * Consumers are assumed to run in the same JVM.
 * <p>
 * Implementations must be thread-safe.
 */
public interface PriceConsumerService {

    /**
     * Returns the latest price records for the given set of instrument IDs.
     * <p>
     * Only completed batches are visible. The returned map is a snapshot and is immutable.
     * Missing IDs will not appear in the map.
     *
     * @param ids the set of instrument IDs to fetch
     * @return a map from instrument ID to immutable payload map
     */
    ImmutableMap<String, ImmutableMap<String, Object>> getLatest(Set<String> ids);

}
