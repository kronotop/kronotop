package com.kronotop.redis.management.coordinator;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class RedisCoordinatorTest {
    @Test
    public void test() throws InterruptedException {
        try (ExecutorService service = Executors.newSingleThreadExecutor()) {
            CountDownLatch latch = new CountDownLatch(2);
            service.submit(() -> {
                try {
                    System.out.println("start 1");
                } finally {
                    latch.countDown();
                }
            });

            service.submit(() -> {
                try {
                    System.out.println("start 2");
                } finally {
                    latch.countDown();
                }
            });

            latch.await();
        }
    }
}