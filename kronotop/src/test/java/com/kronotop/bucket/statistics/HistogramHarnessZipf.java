package com.kronotop.bucket.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

class ZipfGenerator {
    private final int size;
    private final double alpha;
    private final double zetaN;
    private final Random rand;

    public ZipfGenerator(int size, double alpha) {
        this.size = size;
        this.alpha = alpha;
        this.rand = new Random();

        double sum = 0.0;
        for (int i = 1; i <= size; i++) {
            sum += 1.0 / Math.pow(i, alpha);
        }
        this.zetaN = sum;
    }

    public int next() {
        double u = rand.nextDouble();
        double sum = 0.0;
        for (int i = 1; i <= size; i++) {
            sum += (1.0 / Math.pow(i, alpha)) / zetaN;
            if (sum >= u) {
                return i - 1;
            }
        }
        return size - 1;
    }
}


public class HistogramHarnessZipf {
    public static void main(String[] args) {
        int numDocs = 200_000;
        int keyLength = 8;
        int numQueries = 500;
        int domainSize = 100_000; // universe of possible values
        double alpha = 2.0;       // skew parameter

        PrefixHistogram hist = new PrefixHistogram();
        List<byte[]> dataset = new ArrayList<>();
        ZipfGenerator zipf = new ZipfGenerator(domainSize, alpha);

        // === Insert Zipf-distributed keys ===
        for (int i = 0; i < numDocs; i++) {
            int val = zipf.next();
            byte[] k = encodeIntToBytes(val, keyLength);
            dataset.add(k);
            hist.insert(k);
        }

        System.out.println("Inserted docs: " + hist.getTotalDocs());

        // === Run queries ===
        List<Double> errors = new ArrayList<>();

        for (int q = 0; q < numQueries; q++) {
            byte[] A = encodeIntToBytes(ThreadLocalRandom.current().nextInt(domainSize), keyLength);
            byte[] B = encodeIntToBytes(ThreadLocalRandom.current().nextInt(domainSize), keyLength);

            if (compareUnsigned(A, B) >= 0) {
                byte[] tmp = A;
                A = B;
                B = tmp;
            }

            long est = hist.estimateRange(A, B);
            long truth = bruteForceCount(dataset, A, B);

            double error = (truth == 0)
                    ? (est == 0 ? 0.0 : 1.0)
                    : Math.abs(est - truth) / (double) truth;
            errors.add(error);
        }

        Collections.sort(errors);
        double p50 = percentile(errors, 50);
        double p90 = percentile(errors, 90);
        double p99 = percentile(errors, 99);

        System.out.printf("Zipf(α=%.1f) dataset results:%n", alpha);
        System.out.printf("P50 error = %.2f%%%n", p50 * 100);
        System.out.printf("P90 error = %.2f%%%n", p90 * 100);
        System.out.printf("P99 error = %.2f%%%n", p99 * 100);
    }

    private static byte[] encodeIntToBytes(int val, int length) {
        byte[] out = new byte[length];
        for (int i = length - 1; i >= 0; i--) {
            out[i] = (byte) (val & 0xFF);
            val >>>= 8;
        }
        return out;
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
