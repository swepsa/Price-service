package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimaryPriceStorageConcurrencyTest {

    private PrimaryPriceStorage storage;

    @BeforeEach
    void setUp() {
        storage = new PrimaryPriceStorageImpl();
    }

    @RepeatedTest(10)
    void concurrentUpdates_shouldKeepNewestRecord() throws Exception {

        int threads = 20;
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {

            CyclicBarrier barrier = new CyclicBarrier(threads);

            PriceData older = new PriceData(
                    "A",
                    Instant.now().minusSeconds(10),
                    ImmutableMap.of("v", 1)
            );

            PriceData newer = new PriceData(
                    "A",
                    Instant.now(),
                    ImmutableMap.of("v", 2)
            );

            List<Callable<Void>> tasks = new ArrayList<>(threads);

            for (int i = 0; i < threads; i++) {
                tasks.add(() -> {
                    barrier.await();
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        storage.updateRecords(List.of(older));
                    } else {
                        storage.updateRecords(List.of(newer));
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                assertDoesNotThrow(() -> future.get());
            }

            executor.shutdown();
            boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);

            assertTrue(finished, "Executor did not terminate normally — possible deadlock");
        }

        await().atMost(2, SECONDS).until(() ->
                !storage.getLatest(Set.of("A")).isEmpty()
        );
        ImmutableMap<String, ImmutableMap<String, Object>> result =
                storage.getLatest(Set.of("A"));

        assertTrue(result.containsKey("A"), "Record for A must be present");

        ImmutableMap<String, Object> entry = result.get("A");
        assertNotNull(entry, "Payload for A must not be null");
        assertEquals(2, entry.get("v"), "Newest value must win");
    }

}

