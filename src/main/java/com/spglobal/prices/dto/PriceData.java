package com.spglobal.prices.dto;

import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable DTO representing a price record.
 *
 * <p>Business requirements:
 * <ul>
 *     <li>id: a string field to indicate which instrument this price refers to.</li>
 *     <li>asOf: a date-time field to indicate when the price was determined.</li>
 *     <li>payload: the price data itself, which is a flexible data structure.</li>
 * </ul>
 * </p>
 *
 * <p>This class is fully immutable and safe for concurrent usage.</p>
 */
public record PriceData(String id,
                        Instant asOf,
                        ImmutableMap<String, Object> payload) {
    public PriceData {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(asOf, "asOf cannot be null");
        payload = payload == null ? ImmutableMap.of() : ImmutableMap.copyOf(payload);
    }
}
