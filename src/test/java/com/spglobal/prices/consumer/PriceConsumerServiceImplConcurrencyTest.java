package com.spglobal.prices.consumer;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.storage.PrimaryPriceStorage;
import com.spglobal.prices.storage.PrimaryPriceStorageImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PriceConsumerServiceImplConcurrencyTest {

    private PriceConsumerServiceImpl consumer;

    @BeforeEach
    void setup() {
        PrimaryPriceStorage primary = new PrimaryPriceStorageImpl();
        consumer = new PriceConsumerServiceImpl(primary);

        primary.updateRecords(List.of(
                new PriceData("A", Instant.now(), ImmutableMap.of("v", 1))
        ));
    }

    @Test
    void concurrentReads_shouldAlwaysSeeConsistentSnapshot() throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(20)) {

            List<Callable<Void>> tasks = new ArrayList<>();
            for (int j = 0; j < 20; j++) {
                tasks.add(() -> {
                            for (int i = 0; i < 1000; i++) {
                                var data = consumer.getLatest(Set.of("A"));
                                assertTrue(data.containsKey("A"), "Missing key 'A' in snapshot");
                            }
                            return null;
                        }
                );
            }

            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                future.get();
            }

            executor.shutdown();

            boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);
            assertTrue(finished, "Executor did not finish in time");
        }
    }
}

