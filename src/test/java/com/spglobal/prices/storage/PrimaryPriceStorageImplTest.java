package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    void testUpdateRecords_SingleRecord() throws InterruptedException {
        PriceData price = new PriceData("A", Instant.now(), ImmutableMap.of("price", 100));
        storage.updateRecords(List.of(price));

        Thread.sleep(100);

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A"));
        assertEquals(1, result.size());
        ImmutableMap<String, Object> actual = result.get("A");
        assertNotNull(actual);
        assertEquals(100, actual.get("price"));
    }

    @Test
    void testUpdateRecords_keepsLatestByAsOf() throws InterruptedException {
        Instant now = Instant.now();
        PriceData oldPrice = new PriceData("A", now.minusSeconds(10), ImmutableMap.of("price", 50));
        PriceData newPrice = new PriceData("A", now, ImmutableMap.of("price", 100));

        storage.updateRecords(List.of(oldPrice));
        storage.updateRecords(List.of(newPrice));

        Thread.sleep(200);

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A"));
        assertEquals(1, result.size());
        ImmutableMap<String, Object> actual = result.get("A");
        assertNotNull(actual);
        assertEquals(100, actual.get("price"));
    }

    @Test
    void testUpdateRecords_incorrectProducerOrdering() throws InterruptedException {
        Instant now = Instant.now();
        PriceData oldPrice = new PriceData("A", now.minusSeconds(10), ImmutableMap.of("price", 50));
        PriceData newPrice = new PriceData("A", now, ImmutableMap.of("price", 100));

        storage.updateRecords(List.of(newPrice));
        storage.updateRecords(List.of(oldPrice));

        Thread.sleep(200);

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A"));
        assertEquals(1, result.size());
        ImmutableMap<String, Object> actual = result.get("A");
        assertNotNull(actual);
        assertEquals(100, actual.get("price"));
    }

    @Test
    void testUpdateRecords_ignoresNullOrEmpty() throws InterruptedException {
        storage.updateRecords(null);
        storage.updateRecords(List.of());

        Thread.sleep(100);

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("A"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetLatest_ignoresUnknownIds() throws InterruptedException {
        PriceData price = new PriceData("A", Instant.now(), ImmutableMap.of("price", 100));
        storage.updateRecords(List.of(price));

        Thread.sleep(100);

        ImmutableMap<String, ImmutableMap<String, Object>> result = storage.getLatest(Set.of("B"));
        assertTrue(result.isEmpty());
    }


    @Test
    void testGetLatest_filtersNullIds() throws InterruptedException {
        PriceData price = new PriceData("A", Instant.now(), ImmutableMap.of("price", 100));
        storage.updateRecords(List.of(price));

        Thread.sleep(100);

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
