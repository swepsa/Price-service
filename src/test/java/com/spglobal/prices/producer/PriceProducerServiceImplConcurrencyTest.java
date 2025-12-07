package com.spglobal.prices.producer;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.storage.PriceBatchBufferStorage;
import com.spglobal.prices.storage.PriceBatchBufferStorageImpl;
import com.spglobal.prices.storage.PrimaryPriceStorage;
import com.spglobal.prices.storage.PrimaryPriceStorageImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PriceProducerServiceImplConcurrencyTest {

    private PrimaryPriceStorage primary;
    private PriceProducerServiceImpl service;

    @BeforeEach
    void setup() {
        PriceBatchBufferStorage buffer = new PriceBatchBufferStorageImpl();
        primary = new PrimaryPriceStorageImpl();
        service = new PriceProducerServiceImpl(buffer, primary);
    }

    @Test
    void parallelUploadsAndCompletion_shouldWriteAllData() throws Exception {
        String batchId = service.startBatch();

        int threads = 20;
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            CountDownLatch latch = new CountDownLatch(1);

            for (int i = 0; i < threads; i++) {
                int id = i;
                executor.submit(() -> {
                    latch.await();
                    service.uploadChunk(batchId, List.of(
                            new PriceData("A" + id, Instant.now(), ImmutableMap.of("p", id))
                    ));
                    return null;
                });
            }

            latch.countDown();
            executor.shutdown();
            boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);
            assertTrue(finished, "Executor did not finish in time");
        }
        service.completeBatch(batchId);

        Set<String> set = IntStream.range(0, threads)
                                   .mapToObj(i -> "A" + i)
                                   .collect(Collectors.toSet());
        await().atMost(2, SECONDS).until(() ->
                primary.getLatest(set).size() == threads
        );
    }
}

