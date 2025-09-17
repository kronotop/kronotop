package com.kronotop.bucket.statistics;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PrefixHistogram {
    private static final int MAX_DEPTH = 8;

    // Tek byte + depth
    private static class PrefixKey {
        final int depth;
        final byte value;

        PrefixKey(int depth, byte value) {
            this.depth = depth;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PrefixKey)) return false;
            PrefixKey other = (PrefixKey) o;
            return depth == other.depth && value == other.value;
        }

        @Override
        public int hashCode() {
            return 31 * depth + value;
        }
    }

    private final Map<PrefixKey, Long> counters = new ConcurrentHashMap<>();
    private long totalDocs = 0;

    private PrefixKey makePrefixKey(byte[] key, int depth) {
        byte b = (depth <= key.length) ? key[depth - 1] : 0x00;
        return new PrefixKey(depth, b);
    }

    // === Write path ===
    public void insert(byte[] key) {
        int depth = Math.min(key.length, MAX_DEPTH);
        for (int d = 1; d <= depth; d++) {
            PrefixKey pk = makePrefixKey(key, d);
            counters.merge(pk, 1L, Long::sum);
        }
        totalDocs++;
    }

    public void delete(byte[] key) {
        int depth = Math.min(key.length, MAX_DEPTH);
        for (int d = 1; d <= depth; d++) {
            PrefixKey pk = makePrefixKey(key, d);
            counters.merge(pk, -1L, Long::sum);
        }
        totalDocs--;
    }

    public long getTotalDocs() {
        return totalDocs;
    }

    private long count(int depth, int value) {
        return counters.getOrDefault(new PrefixKey(depth, (byte) value), 0L);
    }

    // === Predicates (all via range) ===
    public long estimateLt(byte[] v) {
        return estimateRange(null, v); // (-∞, v)
    }

    public long estimateLte(byte[] v) {
        return estimateRange(null, incrementKey(v)); // (-∞, v]
    }

    public long estimateGt(byte[] v) {
        return estimateRange(incrementKey(v), null); // (v, ∞)
    }

    public long estimateGte(byte[] v) {
        return estimateRange(v, null); // [v, ∞)
    }

    // === Range [A,B) ===
    public long estimateRange(byte[] A, byte[] B) {
        if (A == null && B == null) return totalDocs;

        int a0 = (A != null && A.length > 0) ? (A[0] & 0xFF) : -1;
        int b0 = (B != null && B.length > 0) ? (B[0] & 0xFF) : 256;

        long result = 0;

        if (A == null) {
            // (-∞, B)
            for (int i = 0; i < b0; i++) result += count(1, i);
            if (b0 < 256) {
                long countB = count(1, b0);
                double fracRight = ((B.length > 1) ? (B[1] & 0xFF) : 0) / 256.0;
                result += Math.round(countB * fracRight);
            }
        } else if (B == null) {
            // (A, ∞)
            long countA = count(1, a0);
            double fracLeft = (256.0 - ((A.length > 1) ? (A[1] & 0xFF) : 0)) / 256.0;
            result += Math.round(countA * fracLeft);
            for (int i = a0 + 1; i < 256; i++) result += count(1, i);
        } else {
            // [A,B)
            if (a0 == b0) {
                result += refineRange(A, B, 2);
            } else {
                long countA = count(1, a0);
                double fracLeft = (256.0 - ((A.length > 1) ? (A[1] & 0xFF) : 0)) / 256.0;
                result += Math.round(countA * fracLeft);

                for (int mid = a0 + 1; mid < b0; mid++) {
                    result += count(1, mid);
                }

                if (b0 < 256) {
                    long countB = count(1, b0);
                    double fracRight = ((B.length > 1) ? (B[1] & 0xFF) : 0) / 256.0;
                    result += Math.round(countB * fracRight);
                }
            }
        }

        return result;
    }

    private long refineRange(byte[] A, byte[] B, int depth) {
        if (depth > MAX_DEPTH) return 0;

        int aByte = (A.length >= depth) ? (A[depth - 1] & 0xFF) : 0;
        int bByte = (B.length >= depth) ? (B[depth - 1] & 0xFF) : 0;

        long result = 0;

        if (aByte != bByte) {
            long countA = count(depth, aByte);
            double fracLeft = (256.0 - aByte - 1) / 256.0;
            result += Math.round(countA * fracLeft);

            for (int mid = aByte + 1; mid < bByte; mid++) {
                result += count(depth, mid);
            }

            long countB = count(depth, bByte);
            double fracRight = bByte / 256.0;
            result += Math.round(countB * fracRight);
        } else {
            result += refineRange(A, B, depth + 1);
        }

        return result;
    }

    private byte[] incrementKey(byte[] key) {
        byte[] copy = Arrays.copyOf(key, key.length);
        for (int i = copy.length - 1; i >= 0; i--) {
            int val = (copy[i] & 0xFF);
            if (val != 255) {
                copy[i] = (byte) (val + 1);
                return copy;
            }
            copy[i] = 0;
        }
        return Arrays.copyOf(copy, copy.length + 1);
    }

    // === Demo ===
    public static void main(String[] args) {
        PrefixHistogram hist = new PrefixHistogram();
        hist.insert("ali".getBytes());
        hist.insert("burak".getBytes());
        hist.insert("cemal".getBytes());

        System.out.println("Total docs = " + hist.getTotalDocs());
        System.out.println("> burak = " + hist.estimateGt("burak".getBytes()));
        System.out.println(">= burak = " + hist.estimateGte("burak".getBytes()));
        System.out.println("< cemal = " + hist.estimateLt("cemal".getBytes()));
        System.out.println("<= cemal = " + hist.estimateLte("cemal".getBytes()));
        System.out.println("Range [b,z) = " + hist.estimateRange("b".getBytes(), "z".getBytes()));
    }
}
