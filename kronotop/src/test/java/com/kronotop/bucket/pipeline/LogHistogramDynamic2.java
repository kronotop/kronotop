package com.kronotop.bucket.pipeline;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Log10 tabanlı histogram (pozitif ve negatif ayrı).
 * Negatifler, mutlak değer üzerinden log10 ile bucket'lanır.
 */
public class LogHistogramDynamic2 {
    private final HistogramCore posHist;
    private final HistogramCore negHist; // magnitude için
    private long zeroCount = 0;

    public LogHistogramDynamic2(int m, int groupSize, int windowDecades, int shardCount) {
        this.posHist = new HistogramCore(m, groupSize, windowDecades, shardCount);
        this.negHist = new HistogramCore(m, groupSize, windowDecades, shardCount);
    }

    /* ===================== Demo ===================== */
    public static void main(String[] args) {
        LogHistogramDynamic2 hist = new LogHistogramDynamic2(16, 4, 8, 16);

        double[] values = {-30, -40, -99, -123, -250, -999, -2587, -4589, -10000, 250, 500};
        for (double v : values) hist.add(v);

        hist.printDebug();

        System.out.printf("%nP(> -45)  = %.6f%n", hist.estimateGreaterThan(-45));
        System.out.printf("P(> 200)  = %.6f%n", hist.estimateGreaterThan(200));
        System.out.printf("P(> -500) = %.6f%n", hist.estimateGreaterThan(-500));
        System.out.printf("P([100,500)) = %.6f%n", hist.estimateRange(100, 500));
        /*
            === POSITIVE HIST ===
            d=2 j=6 [237.14, 273.84): 1
            d=2 j=11 [486.97, 562.34): 1
            decadeSum: {2=2}
            underflowSum=0, overflowSum=0
            total=2
            === NEGATIVE HIST (magnitudes) ===
            d=1 j=7 [27.38, 31.62): 1
            d=1 j=9 [36.52, 42.17): 1
            d=1 j=15 [86.60, 100.00): 1
            d=2 j=1 [115.48, 133.35): 1
            d=2 j=6 [237.14, 273.84): 1
            d=2 j=15 [865.96, 1000.00): 1
            d=3 j=6 [2371.37, 2738.42): 1
            d=3 j=10 [4216.97, 4869.68): 1
            d=4 j=0 [10000.00, 11547.82): 1
            decadeSum: {1=3, 2=3, 3=2, 4=1}
            underflowSum=0, overflowSum=0
            total=9
            zeroCount=0, total=11

            P(> -45)  = 0.363636
            P(> 200)  = 0.181818
            P(> -500) = 0.636364
            P([100,500)) = 0.090909
         */
    }

    /**
     * Değeri histogram'a ekle
     */
    public void add(double v) {
        if (v > 0) {
            posHist.add(v);
        } else if (v < 0) {
            negHist.add(-v); // magnitude
        } else {
            zeroCount++;
        }
    }

    /**
     * P(value > t)
     */
    public double estimateGreaterThan(double t) {
        long total = totalCount();
        if (total == 0) return 0.0;

        if (t > 0) {
            long posAbove = posHist.estimateGreaterThanCount(t);
            return (double) posAbove / total;
        } else if (t == 0) {
            return (double) posHist.totalCount() / total;
        } else { // t < 0
            long posTot = posHist.totalCount();
            long negTot = negHist.totalCount();
            long negGE = negHist.estimateGreaterOrEqualCount(Math.abs(t));
            long negLT = negTot - negGE; // |v| < |t|
            long above = posTot + negLT;
            return (double) above / total;
        }
    }

    /**
     * P(a <= value < b)
     */
    public double estimateRange(double a, double b) {
        if (a >= b) return 0.0;
        double geA = estimateGreaterThan(a - 1e-12);
        double geB = estimateGreaterThan(b - 1e-12);
        return Math.max(0.0, geA - geB);
    }

    private long totalCount() {
        return posHist.totalCount() + negHist.totalCount() + zeroCount;
    }

    public void printDebug() {
        System.out.println("=== POSITIVE HIST ===");
        posHist.printDebug();
        System.out.println("=== NEGATIVE HIST (magnitudes) ===");
        negHist.printDebug();
        System.out.println("zeroCount=" + zeroCount + ", total=" + totalCount());
    }

    /* ===================== Inner Core Class ===================== */
    private static class HistogramCore {
        private final int m;
        private final int groupSize;
        private final int groupsPerDecade;
        private final int windowDecades;
        private final int shardCount;

        private final TreeMap<Integer, long[]> counts = new TreeMap<>();
        private final TreeMap<Integer, Long> decadeSum = new TreeMap<>();
        private final TreeMap<Integer, long[]> groupSum = new TreeMap<>();
        private final long[] totalShards;
        private final Random rnd = new Random();
        private long underflowSum = 0;
        private long overflowSum = 0;

        HistogramCore(int m, int groupSize, int windowDecades, int shardCount) {
            this.m = m;
            this.groupSize = groupSize;
            this.groupsPerDecade = m / groupSize;
            this.windowDecades = windowDecades;
            this.shardCount = shardCount;
            this.totalShards = new long[shardCount];
        }

        void add(double v) {
            double log = Math.log10(v);
            int d = (int) Math.floor(log);
            int j = bucketIndexWithinDecade(log, d);
            ensureActiveDecade(d);
            counts.get(d)[j]++;
            decadeSum.put(d, decadeSum.getOrDefault(d, 0L) + 1);
            groupSum.get(d)[j / groupSize]++;
            addToTotal(1);
        }

        long estimateGreaterThanCount(double t) {
            if (totalCount() == 0) return 0;
            double logT = Math.log10(t);
            int dT = (int) Math.floor(logT);
            int jT = bucketIndexWithinDecade(logT, dT);
            int gT = jT / groupSize;

            long countAbove = overflowSum;

            for (Map.Entry<Integer, Long> e : decadeSum.tailMap(dT + 1, true).entrySet()) {
                countAbove += e.getValue();
            }

            Long sumAtDT = decadeSum.get(dT);
            if (sumAtDT != null) {
                long[] garr = groupSum.get(dT);
                for (int g = gT + 1; g < groupsPerDecade; g++) {
                    countAbove += garr[g];
                }
                long[] row = counts.get(dT);
                int start = gT * groupSize;
                int end = Math.min(start + groupSize - 1, m - 1);
                for (int j = Math.max(jT + 1, start); j <= end; j++) {
                    countAbove += row[j];
                }
                long c = row[jT];
                if (c > 0) {
                    double lower = dT + (double) jT / m;
                    double upper = dT + (double) (jT + 1) / m;
                    double ratio = (upper - logT) / (upper - lower);
                    ratio = Math.max(0, Math.min(1, ratio));
                    countAbove += Math.round(c * ratio);
                }
            }
            return countAbove;
        }

        long estimateGreaterOrEqualCount(double t) {
            if (totalCount() == 0) return 0;
            // >=T: ">" ile aynı, sadece bucket'ın tamamını say
            double logT = Math.log10(t);
            int dT = (int) Math.floor(logT);
            int jT = bucketIndexWithinDecade(logT, dT);
            int gT = jT / groupSize;

            long countGE = overflowSum;

            for (Map.Entry<Integer, Long> e : decadeSum.tailMap(dT + 1, true).entrySet()) {
                countGE += e.getValue();
            }

            Long sumAtDT = decadeSum.get(dT);
            if (sumAtDT != null) {
                long[] garr = groupSum.get(dT);
                for (int g = gT + 1; g < groupsPerDecade; g++) {
                    countGE += garr[g];
                }
                long[] row = counts.get(dT);
                int start = gT * groupSize;
                int end = Math.min(start + groupSize - 1, m - 1);
                for (int j = Math.max(jT, start); j <= end; j++) { // j>=jT
                    countGE += row[j];
                }
            }
            return countGE;
        }

        private void ensureActiveDecade(int dNew) {
            if (counts.containsKey(dNew)) return;
            counts.put(dNew, new long[m]);
            decadeSum.put(dNew, 0L);
            groupSum.put(dNew, new long[groupsPerDecade]);
            while (counts.size() > windowDecades) {
                Integer min = counts.firstKey();
                Integer max = counts.lastKey();
                if (dNew <= min) {
                    evictDecade(max, true);
                } else {
                    evictDecade(min, false);
                }
            }
        }

        private void evictDecade(int d, boolean toOverflow) {
            long sum = decadeSum.getOrDefault(d, 0L);
            if (sum > 0) {
                if (toOverflow) overflowSum += sum;
                else underflowSum += sum;
            }
            counts.remove(d);
            decadeSum.remove(d);
            groupSum.remove(d);
        }

        private void addToTotal(long delta) {
            int s = rnd.nextInt(shardCount);
            totalShards[s] += delta;
        }

        long totalCount() {
            long s = 0;
            for (long v : totalShards) s += v;
            return s;
        }

        private int bucketIndexWithinDecade(double logV, int d) {
            double frac = logV - d;
            int j = (int) Math.floor(m * frac);
            if (j < 0) j = 0;
            if (j >= m) j = m - 1;
            return j;
        }

        void printDebug() {
            for (Map.Entry<Integer, long[]> e : counts.entrySet()) {
                int d = e.getKey();
                long[] row = e.getValue();
                for (int j = 0; j < m; j++) {
                    if (row[j] == 0) continue;
                    double lower = Math.pow(10, d + (double) j / m);
                    double upper = Math.pow(10, d + (double) (j + 1) / m);
                    System.out.printf("d=%d j=%d [%.2f, %.2f): %d%n", d, j, lower, upper, row[j]);
                }
            }
            System.out.println("decadeSum: " + decadeSum);
            System.out.println("underflowSum=" + underflowSum + ", overflowSum=" + overflowSum);
            System.out.println("total=" + totalCount());
        }
    }
}
