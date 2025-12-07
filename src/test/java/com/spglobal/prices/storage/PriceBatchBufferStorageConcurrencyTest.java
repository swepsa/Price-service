package com.spglobal.prices.storage;

import com.google.common.collect.ImmutableMap;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.exception.NoSuchBatchException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PriceBatchBufferStorageConcurrencyTest {

    private PriceBatchBufferStorage buffer;

    @BeforeEach
    void setup() {
        buffer = new PriceBatchBufferStorageImpl();
    }

    @Test
    void concurrentChunkUpload_shouldAggregateAllChunks() throws Exception {
        String batch = buffer.startBatch();

        int threads = 30;
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            CountDownLatch latch = new CountDownLatch(1);

            for (int i = 0; i < threads; i++) {
                int index = i;
                executor.submit(() -> {
                    latch.await();
                    buffer.addChunk(batch,
                            List.of(new PriceData("id" + index, Instant.now(), ImmutableMap.of("x", index)))
                    );
                    return null;
                });
            }

            latch.countDown();
            executor.shutdown();
            boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);
            assertTrue(finished, "Executor did not finish in time");
        }
        List<PriceData> result = buffer.completeBatch(batch);

        assertEquals(threads, result.size());
    }

    @Test
    void cancelDuringConcurrentUploads_shouldNotProduceBatch() throws Exception {
        String batch = buffer.startBatch();

        try (ExecutorService executor = Executors.newFixedThreadPool(10)) {
            List<Callable<Boolean>> tasks = getCallables(batch);

            List<Future<Boolean>> futures = executor.invokeAll(tasks);

            int positiveCompletion = 0;
            for (Future<Boolean> future : futures) {
                if (future.get()) {
                    positiveCompletion++;
                }
            }
            assertTrue(positiveCompletion >= 2);

            executor.shutdown();
            boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);
            assertTrue(finished, "Executor did not finish in time");
        }
        assertThrows(NoSuchBatchException.class, () -> buffer.completeBatch(batch));
    }

    private List<Callable<Boolean>> getCallables(String batch) {
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(() -> {
                try {
                    buffer.addChunk(batch, List.of(
                            new PriceData("X", Instant.now(), ImmutableMap.of())
                    ));
                } catch (NoSuchBatchException ignore) {
                    return false;
                }
                return true;
            });
        }

        tasks.add(() -> {
            buffer.cancelBatch(batch);
            return true;
        });
        return tasks;
    }
}

