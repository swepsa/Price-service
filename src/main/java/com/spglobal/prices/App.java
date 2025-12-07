package com.spglobal.prices;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spglobal.prices.consumer.PriceConsumerService;
import com.spglobal.prices.consumer.PriceConsumerServiceImpl;
import com.spglobal.prices.dto.PriceData;
import com.spglobal.prices.producer.PriceProducerService;
import com.spglobal.prices.producer.PriceProducerServiceImpl;
import com.spglobal.prices.storage.PriceBatchBufferStorage;
import com.spglobal.prices.storage.PriceBatchBufferStorageImpl;
import com.spglobal.prices.storage.PrimaryPriceStorage;
import com.spglobal.prices.storage.PrimaryPriceStorageImpl;

import java.time.Instant;
import java.util.List;

public class App {
    public static void main(String[] args) throws InterruptedException {
        PriceBatchBufferStorage bufferStorage = new PriceBatchBufferStorageImpl();
        PrimaryPriceStorage storage = new PrimaryPriceStorageImpl();
        PriceProducerService producerService = new PriceProducerServiceImpl(bufferStorage, storage);
        PriceConsumerService consumerService = new PriceConsumerServiceImpl(storage);

        String batchId = producerService.startBatch();
        producerService.uploadChunk(batchId, getCollection1());
        producerService.uploadChunk(batchId, getCollection2());
        producerService.completeBatch(batchId);
        Thread.sleep(100L);
        System.out.println(consumerService.getLatest(ImmutableSet.of("Sp1")));

    }

    private static List<PriceData> getCollection1() {
        return List.of(new PriceData("Sp1", Instant.now().minusSeconds(10), ImmutableMap.of("ask", 123)));
    }

    private static List<PriceData> getCollection2() {
        return List.of(new PriceData("Sp1", Instant.now(), ImmutableMap.of("ask", 124)));
    }
}
