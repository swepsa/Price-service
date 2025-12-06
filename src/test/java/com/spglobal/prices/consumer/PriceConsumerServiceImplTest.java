package com.spglobal.prices.consumer;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.storage.PrimaryPriceStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PriceConsumerServiceImplTest {

    private PrimaryPriceStorage storage;
    private PriceConsumerServiceImpl service;

    @BeforeEach
    void setUp() {
        storage = mock(PrimaryPriceStorage.class);
        service = new PriceConsumerServiceImpl(storage);
    }


    @Test
    void testConstructor_npeThrows() {
        assertThrows(NullPointerException.class, () -> new PriceConsumerServiceImpl(null));
    }


    @Test
    void testGetLatest_nullIds_returnsEmptyMap() {
        ImmutableMap<String, ImmutableMap<String, Object>> result = service.getLatest(null);

        assertTrue(result.isEmpty());
        verify(storage, never()).getLatest(any());
    }

    @Test
    void testGetLatest_emptyIds_returnsEmptyMap() {
        ImmutableMap<String, ImmutableMap<String, Object>> result = service.getLatest(Set.of());

        assertTrue(result.isEmpty());
        verify(storage, never()).getLatest(any());
    }

    @Test
    void testGetLatest_delegatesToStorage() {
        Set<String> ids = Set.of("A", "B");

        ImmutableMap<String, ImmutableMap<String, Object>> expected =
                ImmutableMap.of(
                        "A", ImmutableMap.of("x", 1),
                        "B", ImmutableMap.of("y", 2)
                );

        when(storage.getLatest(ids)).thenReturn(expected);

        ImmutableMap<String, ImmutableMap<String, Object>> result = service.getLatest(ids);

        verify(storage).getLatest(ids);
        assertEquals(expected, result);
    }

}
