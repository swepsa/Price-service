package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimaryPriceStorageImplTest {

    private PrimaryPriceStorageImpl storage;

    @BeforeEach
    void setUp() {
        storage = new PrimaryPriceStorageImpl();
    }

    @Test
    void testGetLatest_EmptyStorage_returnsEmptyCollection() {
        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A", "B"));
        assertTrue(result.isEmpty(), "Storage should be empty initially");
    }

    @Test
    void testUpdateRecords_SingleRecord() {
        PriceData price = new PriceData("A", Instant.now(), ImmutableMap.of("price", 100));
        storage.updateRecords(List.of(price));

        await().atMost(2, SECONDS).until(() ->
                !storage.getLatest(Set.of("A")).isEmpty()
        );

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A"));
        assertEquals(1, result.size());
        ImmutableMap<String, Object> actual = result.get("A");
        assertNotNull(actual);
        assertEquals(100, actual.get("price"));
    }

    @Test
    void testUpdateRecords_keepsLatestByAsOf() {
        Instant now = Instant.now();
        PriceData oldPrice = new PriceData("A", now.minusSeconds(10), ImmutableMap.of("price", 50));
        PriceData newPrice = new PriceData("A", now, ImmutableMap.of("price", 100));

        storage.updateRecords(List.of(oldPrice));
        storage.updateRecords(List.of(newPrice));

        await().atMost(2, SECONDS).until(() ->
                !storage.getLatest(Set.of("A")).isEmpty()
        );

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A"));
        assertEquals(1, result.size());
        ImmutableMap<String, Object> actual = result.get("A");
        assertNotNull(actual);
        assertEquals(100, actual.get("price"));
    }

    @Test
    void testUpdateRecords_incorrectProducerOrdering() {
        Instant now = Instant.now();
        PriceData oldPrice = new PriceData("A", now.minusSeconds(10), ImmutableMap.of("price", 50));
        PriceData newPrice = new PriceData("A", now, ImmutableMap.of("price", 100));
        PriceData price = new PriceData("B", now, ImmutableMap.of("price", 1));

        storage.updateRecords(List.of(newPrice));
        storage.updateRecords(List.of(price, oldPrice));

        await().atMost(2, SECONDS).until(() ->
                !storage.getLatest(Set.of("B")).isEmpty()
        );

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A"));
        assertEquals(1, result.size());
        ImmutableMap<String, Object> actual = result.get("A");
        assertNotNull(actual);
        assertEquals(100, actual.get("price"));
    }

    @Test
    void testUpdateRecords_ignoresNullOrEmpty() {
        storage.updateRecords(null);
        storage.updateRecords(List.of());

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetLatest_ignoresUnknownIds() {
        PriceData price = new PriceData("A", Instant.now(), ImmutableMap.of("price", 100));
        storage.updateRecords(List.of(price));

        await().atMost(2, SECONDS).until(() ->
                !storage.getLatest(Set.of("A")).isEmpty()
        );

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("B"));
        assertTrue(result.isEmpty());
    }


    @Test
    void testGetLatest_filtersNullIds() {
        PriceData price = new PriceData("A", Instant.now(), ImmutableMap.of("price", 100));
        storage.updateRecords(List.of(price));

        await().atMost(2, SECONDS).until(() ->
                !storage.getLatest(Set.of("A")).isEmpty()
        );

        Set<String> set = new HashSet<>() {
        };
        set.add("A");
        set.add(null);
        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(set);
        assertEquals(1, result.size());
        ImmutableMap<String, Object> actual = result.get("A");
        assertNotNull(actual);
        assertEquals(100, actual.get("price"));
    }
}
