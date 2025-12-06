package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe implementation of {@link PrimaryPriceStorage}.
 * <p>
 * Supports atomic snapshot reads and single-threaded batch updates.
 */
public class PrimaryPriceStorageImpl implements PrimaryPriceStorage {
    private final AtomicReference<ImmutableMap<String, PriceData>> storage = new AtomicReference<>();

    private final ExecutorService singleWriter = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    public PrimaryPriceStorageImpl() {
        storage.set(ImmutableMap.of());
    }

    @Override
    public ImmutableMap<String, ImmutableMap<String, Object>> getLatest(Set<String> ids) {
        Map<String, PriceData> snapshot = storage.get();
        return ids.stream()
                  .filter(Objects::nonNull)
                  .map(snapshot::get)
                  .filter(Objects::nonNull)
                  .collect(ImmutableMap.toImmutableMap(PriceData::id, PriceData::payload));
    }

    @Override
    public void updateRecords(List<PriceData> priceDataList) {
        if (priceDataList == null || priceDataList.isEmpty()) {
            return;
        }
        singleWriter.submit(() -> {
            Map<String, PriceData> snapshot = new HashMap<>(storage.get());

            for (PriceData price : priceDataList) {
                snapshot.merge(price.id(), price,
                        (oldValue, newValue) -> newValue.asOf().isAfter(oldValue.asOf()) ? newValue : oldValue);
            }

            storage.set(ImmutableMap.copyOf(snapshot));
        });
    }
}
