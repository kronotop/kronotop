package com.kronotop.bucket.statistics;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class HistogramHarness {

    public static void main(String[] args) {
        int numDocs = 200_000;
        int keyLength = 8;
        int numQueries = 500;

        PrefixHistogram hist = new PrefixHistogram();
        List<byte[]> dataset = new ArrayList<>();

        // Insert random uniform keys
        for (int i = 0; i < numDocs; i++) {
            byte[] k = randomKey(keyLength);
            dataset.add(k);
            hist.insert(k);
        }

        System.out.println("Inserted docs: " + hist.getTotalDocs());

        // Collect errors for each predicate
        testPredicates(hist, dataset, numQueries);
    }

    private static void testPredicates(PrefixHistogram hist, List<byte[]> dataset, int numQueries) {
        List<Double> gtErrors = new ArrayList<>();
        List<Double> gteErrors = new ArrayList<>();
        List<Double> ltErrors = new ArrayList<>();
        List<Double> lteErrors = new ArrayList<>();
        List<Double> rangeErrors = new ArrayList<>();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int q = 0; q < numQueries; q++) {
            byte[] v = dataset.get(rnd.nextInt(dataset.size()));
            byte[] A = randomKey(8);
            byte[] B = randomKey(8);
            if (compareUnsigned(A, B) >= 0) {
                byte[] tmp = A;
                A = B;
                B = tmp;
            }

            // >
            long estGt = hist.estimateGt(v);
            long truthGt = bruteForceGt(dataset, v);
            gtErrors.add(error(estGt, truthGt));

            // >=
            long estGte = hist.estimateGte(v);
            long truthGte = bruteForceGte(dataset, v);
            gteErrors.add(error(estGte, truthGte));

            // <
            long estLt = hist.estimateLt(v);
            long truthLt = bruteForceLt(dataset, v);
            ltErrors.add(error(estLt, truthLt));

            // <=
            long estLte = hist.estimateLte(v);
            long truthLte = bruteForceLte(dataset, v);
            lteErrors.add(error(estLte, truthLte));

            // Range
            long estRange = hist.estimateRange(A, B);
            long truthRange = bruteForceRange(dataset, A, B);
            rangeErrors.add(error(estRange, truthRange));
        }

        report("Gt", gtErrors);
        report("Gte", gteErrors);
        report("Lt", ltErrors);
        report("Lte", lteErrors);
        report("Range", rangeErrors);
    }

    private static void report(String name, List<Double> errors) {
        Collections.sort(errors);
        System.out.printf("%s: P50=%.2f%%, P90=%.2f%%, P99=%.2f%%%n",
                name,
                errors.get((int)(errors.size() * 0.50)) * 100,
                errors.get((int)(errors.size() * 0.90)) * 100,
                errors.get((int)(errors.size() * 0.99)) * 100);
    }

    private static double error(long est, long truth) {
        if (truth == 0) return est == 0 ? 0.0 : 1.0;
        return Math.abs(est - truth) / (double) truth;
    }

    // === Random keys ===
    private static byte[] randomKey(int len) {
        byte[] k = new byte[len];
        ThreadLocalRandom.current().nextBytes(k);
        return k;
    }

    // === Comparisons ===
    private static int compareUnsigned(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int ai = a[i] & 0xFF, bi = b[i] & 0xFF;
            if (ai != bi) return ai - bi;
        }
        return a.length - b.length;
    }

    // === Brute force truth ===
    private static long bruteForceGt(List<byte[]> data, byte[] v) {
        return data.stream().filter(k -> compareUnsigned(k, v) > 0).count();
    }

    private static long bruteForceGte(List<byte[]> data, byte[] v) {
        return data.stream().filter(k -> compareUnsigned(k, v) >= 0).count();
    }

    private static long bruteForceLt(List<byte[]> data, byte[] v) {
        return data.stream().filter(k -> compareUnsigned(k, v) < 0).count();
    }

    private static long bruteForceLte(List<byte[]> data, byte[] v) {
        return data.stream().filter(k -> compareUnsigned(k, v) <= 0).count();
    }

    private static long bruteForceRange(List<byte[]> data, byte[] A, byte[] B) {
        return data.stream()
                .filter(k -> compareUnsigned(k, A) >= 0 && compareUnsigned(k, B) < 0)
                .count();
    }
}
