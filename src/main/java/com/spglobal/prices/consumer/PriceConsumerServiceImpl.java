package com.spglobal.prices.consumer;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.storage.PrimaryPriceStorage;

import java.util.Objects;
import java.util.Set;

/**
 * Implementation of PriceConsumerService using PrimaryPriceStorage.
 * <p>
 * Thread-safe. Returns immutable snapshots of the latest price records.
 */
public class PriceConsumerServiceImpl implements PriceConsumerService {

    private final PrimaryPriceStorage storage;

    public PriceConsumerServiceImpl(PrimaryPriceStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
    }

    @Override
    public ImmutableMap<String, ImmutableMap<String, Object>> getLatest(Set<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return ImmutableMap.of();
        }
        return storage.getLatest(ids);
    }

}
