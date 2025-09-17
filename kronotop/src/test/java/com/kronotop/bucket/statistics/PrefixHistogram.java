package com.kronotop.bucket.statistics;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PrefixHistogram {
    private static final int MAX_DEPTH = 8;

    // Bin = (depth, byte)
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
        if (depth > key.length) return null;
        return new PrefixKey(depth, key[depth - 1]);
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

    // === Predicates ===
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
        return estimateRangeRec(A, B, 1);
    }

    private long estimateRangeRec(byte[] A, byte[] B, int depth) {
        if (depth > MAX_DEPTH) return 0;

        int aByte = (A != null && A.length >= depth) ? (A[depth - 1] & 0xFF) : -1;
        int bByte = (B != null && B.length >= depth) ? (B[depth - 1] & 0xFF) : 256;

        if (A == null) { // (-∞, B)
            long sum = 0;
            for (int i = 0; i < bByte; i++) sum += count(depth, i);
            if (B != null && B.length > depth) {
                sum += estimateRangeRec(null, B, depth + 1); // refine only B-bin
            }
            return sum;
        }

        if (B == null) { // [A, ∞)
            long sum = 0;
            for (int i = aByte + 1; i < 256; i++) sum += count(depth, i);
            if (A.length > depth) {
                sum += estimateRangeRec(A, null, depth + 1); // refine only A-bin
            }
            return sum;
        }

        // [A,B)
        if (aByte == bByte) {
            return estimateRangeRec(A, B, depth + 1);
        } else {
            long sum = 0;
            // tail of A-bin
            if (A.length > depth) {
                sum += estimateRangeRec(A, null, depth + 1);
            }
            // middle bins
            for (int i = aByte + 1; i < bByte; i++) sum += count(depth, i);
            // head of B-bin
            if (B.length > depth) {
                sum += estimateRangeRec(null, B, depth + 1);
            }
            return sum;
        }
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
