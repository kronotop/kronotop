package com.kronotop.bucket.statistics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrefixHistogram {
    private static final int MAX_DEPTH = 8;
    private final Map<PrefixKey, Long> counters = new ConcurrentHashMap<>();
    private long totalDocs = 0;

    // === simple demo ===
    public static void main(String[] args) {
        PrefixHistogram hist = new PrefixHistogram();

        hist.insert("burak".getBytes());
        hist.insert("cemal".getBytes());
        hist.insert("ali".getBytes());

        System.out.println("Total docs = " + hist.getTotalDocs());
        System.out.println("Eq burak = " + hist.estimateEq("burak".getBytes()));
        System.out.println("Range [b,z) = " + hist.estimateRange("b".getBytes(), "z".getBytes()));
    }

    private PrefixKey makePrefixKey(byte[] key, int depth) {
        int d = Math.min(depth, key.length);
        return new PrefixKey(depth, Arrays.copyOf(key, d));
    }

    // Insert key
    public void insert(byte[] key) {
        int depth = Math.min(key.length, MAX_DEPTH);
        for (int d = 1; d <= depth; d++) {
            PrefixKey pk = makePrefixKey(key, d);
            counters.merge(pk, 1L, Long::sum);
        }
        totalDocs++;
    }

    // Delete key
    public void delete(byte[] key) {
        int depth = Math.min(key.length, MAX_DEPTH);
        for (int d = 1; d <= depth; d++) {
            PrefixKey pk = makePrefixKey(key, d);
            counters.merge(pk, -1L, Long::sum);
        }
        totalDocs--;
    }

    // Equality estimation (=v)
    public double estimateEq(byte[] key) {
        int depth = Math.min(key.length, MAX_DEPTH);
        PrefixKey pk = makePrefixKey(key, depth);
        long count = counters.getOrDefault(pk, 0L);
        return totalDocs == 0 ? 0.0 : (double) count / totalDocs;
    }

    // Range estimation [A,B) (simplified root-level only)
    public long estimateRange(byte[] A, byte[] B) {
        // Build root-level map
        Map<Integer, Long> root = new HashMap<>();
        for (int i = 0; i < 256; i++) {
            PrefixKey pk = new PrefixKey(1, new byte[]{(byte) i});
            root.put(i, counters.getOrDefault(pk, 0L));
        }

        int a0 = (A.length > 0) ? (A[0] & 0xFF) : 0;
        int b0 = (B.length > 0) ? (B[0] & 0xFF) : 0;

        long result = 0;

        if (a0 == b0) {
            // Range fully inside one root bin -> deeper refinement needed
            result += refineRange(A, B, 2);
        } else {
            // Left edge
            long countA = root.getOrDefault(a0, 0L);
            double fracLeft = (256.0 - ((A.length > 1) ? (A[1] & 0xFF) : 0)) / 256.0;
            result += Math.round(countA * fracLeft);

            // Middle bins
            for (int mid = a0 + 1; mid < b0; mid++) {
                result += root.getOrDefault(mid, 0L);
            }

            // Right edge
            long countB = root.getOrDefault(b0, 0L);
            double fracRight = ((B.length > 1) ? (B[1] & 0xFF) : 0) / 256.0;
            result += Math.round(countB * fracRight);
        }

        return result;
    }

    // Recursive refinement for [A,B) inside same prefix
    private long refineRange(byte[] A, byte[] B, int depth) {
        if (depth > MAX_DEPTH) return 0;

        int aByte = (A.length >= depth) ? (A[depth - 1] & 0xFF) : 0;
        int bByte = (B.length >= depth) ? (B[depth - 1] & 0xFF) : 0;

        if (aByte != bByte) {
            long est = 0;

            PrefixKey leftPk = makePrefixKey(A, depth);
            long countA = counters.getOrDefault(leftPk, 0L);
            double fracLeft = (256.0 - aByte - 1) / 256.0;
            est += Math.round(countA * fracLeft);

            for (int mid = aByte + 1; mid < bByte; mid++) {
                PrefixKey midPk = new PrefixKey(depth, Arrays.copyOf(A, depth));
                midPk.prefix[depth - 1] = (byte) mid;
                est += counters.getOrDefault(midPk, 0L);
            }

            PrefixKey rightPk = makePrefixKey(B, depth);
            long countB = counters.getOrDefault(rightPk, 0L);
            double fracRight = bByte / 256.0;
            est += Math.round(countB * fracRight);

            return est;
        } else {
            return refineRange(A, B, depth + 1);
        }
    }

    public long getTotalDocs() {
        return totalDocs;
    }

    // Key wrapper for (depth, prefixBytes)
    private record PrefixKey(int depth, byte[] prefix) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PrefixKey(int depth1, byte[] prefix1))) return false;
            return depth == depth1 && Arrays.equals(prefix, prefix1);
        }

        @Override
        public int hashCode() {
            return 31 * depth + Arrays.hashCode(prefix);
        }
    }
}
