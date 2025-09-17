package com.kronotop.bucket.statistics;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class PrefixHistogramHarness {
    public static void main(String[] args) {
        int numDocs = 200_000;   // number of documents to insert
        int keyLength = 8;       // fixed length for random keys
        int numQueries = 500;    // number of queries to test

        PrefixHistogram hist = new PrefixHistogram();
        List<byte[]> dataset = new ArrayList<>();

        // === Insert random keys (uniform distribution) ===
        for (int i = 0; i < numDocs; i++) {
            byte[] k = randomKey(keyLength);
            dataset.add(k);
            hist.insert(k);
        }

        System.out.println("Inserted docs: " + hist.getTotalDocs());

        // === Run queries and record relative errors ===
        List<Double> errors = new ArrayList<>();

        for (int q = 0; q < numQueries; q++) {
            byte[] A = randomKey(keyLength);
            byte[] B = randomKey(keyLength);

            // Ensure A < B
            if (compareUnsigned(A, B) >= 0) {
                byte[] tmp = A;
                A = B;
                B = tmp;
            }

            long est = hist.estimateRange(A, B);
            long truth = bruteForceCount(dataset, A, B);

            double error = (truth == 0)
                    ? (est == 0 ? 0.0 : 1.0)  // if truth=0, any nonzero estimate = 100% error
                    : Math.abs(est - truth) / (double) truth;

            errors.add(error);
        }

        // === Report percentiles ===
        Collections.sort(errors);
        double p50 = percentile(errors, 50);
        double p90 = percentile(errors, 90);
        double p99 = percentile(errors, 99);

        System.out.printf("Uniform dataset results:%n");
        System.out.printf("P50 error = %.2f%%%n", p50 * 100);
        System.out.printf("P90 error = %.2f%%%n", p90 * 100);
        System.out.printf("P99 error = %.2f%%%n", p99 * 100);
    }

    private static byte[] randomKey(int len) {
        byte[] k = new byte[len];
        ThreadLocalRandom.current().nextBytes(k);
        return k;
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return ai - bi;
        }
        return a.length - b.length;
    }

    private static long bruteForceCount(List<byte[]> data, byte[] A, byte[] B) {
        long cnt = 0;
        for (byte[] k : data) {
            if (compareUnsigned(k, A) >= 0 && compareUnsigned(k, B) < 0) {
                cnt++;
            }
        }
        return cnt;
    }

    private static double percentile(List<Double> sorted, int pct) {
        int idx = (int) Math.floor(sorted.size() * (pct / 100.0));
        idx = Math.min(idx, sorted.size() - 1);
        return sorted.get(idx);
    }
}
