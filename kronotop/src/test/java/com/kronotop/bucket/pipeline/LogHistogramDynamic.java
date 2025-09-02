package com.kronotop.bucket.pipeline;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Log10 tabanlı histogram (production kısıtlarına uygun).
 * <p>
 * - Her dekad (10^d..10^(d+1)) m alt-bucketa (j=0..m-1) bölünür.
 * - Pencere boyutu W: aynı anda en fazla W dekad aktif tutulur.
 * - Okumayı azaltmak için:
 * * dekad toplamı (sum) tutulur,
 * * j'ler grupSize büyüklüğünde "grup toplamı" olarak da tutulur.
 * - Pencereden taşan dekadların toplamları underflow/overflow özetlerine eklenir.
 * - total, şardlı sayılır (FDB'de hot key kaçınma için).
 * <p>
 * NOT: Bu sınıf in-memory. FDB'ye geçerken:
 * counts[d][j]          -> (/stats,price,log10,m,d,j)
 * decadeSum[d]          -> (/stats,price,log10,m,d,"sum")
 * groupSum[d][g]        -> (/stats,price,log10,m,d,"g",g)
 * totalShards[s]        -> (/stats,price,total,s)
 * underflowSum          -> (/stats,price,underflow_sum)
 * overflowSum           -> (/stats,price,overflow_sum)
 * meta(dMin,dMax,m,W,groupSize,shards) -> (/stats,price,meta)
 */
public class LogHistogramDynamic {
    private final int m;                 // alt-bucket sayısı / dekad
    private final int groupSize;         // grup büyüklüğü (m % groupSize == 0)
    private final int groupsPerDecade;   // m / groupSize
    private final int windowDecades;     // aktif dekad penceresi (W)
    private final int shardCount;        // total için şard sayısı

    // Aktif dekadlar: d -> counts[j]
    private final TreeMap<Integer, long[]> counts = new TreeMap<>();
    // Dekad toplamı: d -> sum(counts[d][*])
    private final TreeMap<Integer, Long> decadeSum = new TreeMap<>();
    // Grup toplamları: d -> long[groupsPerDecade]
    private final TreeMap<Integer, long[]> groupSum = new TreeMap<>();
    // Şardlı total
    private final long[] totalShards;
    private final Random rnd = new Random();
    // Pencereden taşan toplamlar (özet)
    private long underflowSum = 0;
    private long overflowSum = 0;
    private long zeroOrNeg = 0;

    public LogHistogramDynamic(int m, int groupSize, int windowDecades, int shardCount) {
        if (m <= 0) throw new IllegalArgumentException("m must be > 0");
        if (groupSize <= 0 || (m % groupSize) != 0)
            throw new IllegalArgumentException("groupSize must divide m");
        if (windowDecades <= 0) throw new IllegalArgumentException("windowDecades must be > 0");
        if (shardCount <= 0) throw new IllegalArgumentException("shardCount must be > 0");

        this.m = m;
        this.groupSize = groupSize;
        this.groupsPerDecade = m / groupSize;
        this.windowDecades = windowDecades;
        this.shardCount = shardCount;
        this.totalShards = new long[shardCount];
    }

    /* ===================== Yazma (O(1), okumasız) ===================== */

    private static double clamp01(double x) {
        if (x < 0) return 0;
        if (x > 1) return 1;
        return x;
    }

    public static void main(String[] args) {
        //         double[] values = {-30, -40, -99, -123, -250, -999, -2587, -4589, -10000};
        // m=16, groupSize=4 (grup başına 4 j), W=8 dekad, total için 16 shard
        LogHistogramDynamic hist = new LogHistogramDynamic(16, 4, 8, 16);

        double[] values = {-30, -40, -99, -123, -250, -999, -2587, -4589, -10000};
        for (double v : values) hist.add(v);

        // pencereyi zorlayalım
        //double[] more = {1.2, 2.5, 6.7, 8.9, 1e6, 3e7, 9e8, 4.2e9};
        //for (double v : more) hist.add(v);

        hist.printDebug();

        System.out.printf("%nP(>25)   = %.6f%n", hist.estimateGreaterThan(-45));
        //System.out.printf("%nP(>50)   = %.6f%n", hist.estimateGreaterThan(50));
        //System.out.printf("P(>200)  = %.6f%n", hist.estimateGreaterThan(200));
        //System.out.printf("P(>3000) = %.6f%n", hist.estimateGreaterThan(3000));
        //System.out.printf("P([100,500)) = %.6f%n", hist.estimateRange(100, 500));
    }

    /* ===================== Tahmin (O(W) ~ sabit) ===================== */

    /**
     * Değeri histogram'a ekle (v<=0 -> zeroOrNeg).
     */
    public void add(double v) {
        if (v <= 0) {
            zeroOrNeg++;
            addToTotal(1);
            return;
        }
        final double log = Math.log10(v);
        final int d = (int) Math.floor(log);
        final int j = bucketIndexWithinDecade(log, d);

        ensureActiveDecade(d); // gerekirse pencereyi kaydırır (özetlere taşır)

        long[] row = counts.get(d);
        row[j]++;

        // dekad toplamı
        decadeSum.put(d, decadeSum.getOrDefault(d, 0L) + 1);

        // grup toplamı
        int g = j / groupSize;
        long[] garr = groupSum.get(d);
        garr[g]++;

        addToTotal(1);
    }

    private void addToTotal(long delta) {
        int s = rnd.nextInt(shardCount);
        totalShards[s] += delta;
    }

    /* ===================== Pencere Yönetimi ===================== */

    /**
     * P( price > t )
     */
    public double estimateGreaterThan(double t) {
        long total = totalCount();
        if (total == 0) return 0.0;
        if (t <= 0) return 1.0;

        double logT = Math.log10(t);
        int dT = (int) Math.floor(logT);
        int jT = bucketIndexWithinDecade(logT, dT);
        int gT = jT / groupSize;

        long countAbove = 0;

        // 1) ÜST UÇ ÖZETİ: pencereden taşmış yüksek dekadlar
        countAbove += overflowSum;

        // 2) Aktif dekadlarda d > dT olanlar: dekad toplamı ile tek seferde
        for (Map.Entry<Integer, Long> e : decadeSum.tailMap(dT + 1, true).entrySet()) {
            countAbove += e.getValue();
        }

        // 3) d == dT olan dekad(lar): grup toplamları ile hızlı sayım + kısmi bucket
        // (aktif haritada birden çok d==dT beklemiyoruz, ama API genel dursa sorun yok)
        Long sumAtDT = decadeSum.get(dT);
        if (sumAtDT != null) {
            long[] garr = groupSum.get(dT);
            // jT'nin üstündeki TAM gruplar (g > gT)
            for (int g = gT + 1; g < groupsPerDecade; g++) {
                countAbove += garr[g];
            }
            // jT ile aynı grupta: gruptaki j>jT olanları tek tek ekle
            long[] row = counts.get(dT);
            int start = gT * groupSize;
            int end = Math.min(start + groupSize - 1, m - 1);
            for (int j = Math.max(jT + 1, start); j <= end; j++) {
                countAbove += row[j];
            }
            // j == jT: kısmi katkı (log-uzayda lineer)
            long c = row[jT];
            if (c > 0) {
                double lower = dT + (double) jT / m;
                double upper = dT + (double) (jT + 1) / m;
                double ratio = (upper > lower) ? (upper - logT) / (upper - lower) : 0.0;
                if (ratio < 0) ratio = 0;
                if (ratio > 1) ratio = 1;
                countAbove += Math.round(c * ratio);
            }
        }

        // 4) ALT UÇ ÖZETİ: underflowSum, >t için katkı yapmaz (d<dT kesin küçük).
        return clamp01((double) countAbove / (double) total);
    }

    /**
     * P( price in [a,b) )  ~  P(>=a) - P(>=b)
     */
    public double estimateRange(double a, double b) {
        long total = totalCount();
        if (total == 0) return 0.0;
        if (b <= 0 || a >= b) return 0.0;

        double eps = 1e-12;
        double geA = estimateGreaterThan(a - eps);
        double geB = estimateGreaterThan(b - eps);
        return clamp01(geA - geB);
    }

    /* ===================== Yardımcılar / Debug ===================== */

    private void ensureActiveDecade(int dNew) {
        if (counts.containsKey(dNew)) return;

        // yeni dekad için kayıtları hazırla
        counts.put(dNew, new long[m]);
        decadeSum.put(dNew, 0L);
        groupSum.put(dNew, new long[groupsPerDecade]);

        // pencere boyutunu koru
        while (counts.size() > windowDecades) {
            Integer min = counts.firstKey();
            Integer max = counts.lastKey();

            // yeni gelen dekada göre uç seçimi: pencereyi "yakın" tut
            if (dNew <= min) {
                // üst ucu taşır: max -> overflow
                evictDecadeToSummary(max, /*toOverflow=*/true);
            } else {
                // alt ucu taşır: min -> underflow
                evictDecadeToSummary(min, /*toOverflow=*/false);
            }
        }
    }

    private void evictDecadeToSummary(int d, boolean toOverflow) {
        long sum = decadeSum.getOrDefault(d, 0L);
        if (sum > 0) {
            if (toOverflow) overflowSum += sum;
            else underflowSum += sum;
        }
        counts.remove(d);
        decadeSum.remove(d);
        groupSum.remove(d);
    }

    private int bucketIndexWithinDecade(double logV, int d) {
        double frac = logV - d; // [0,1)
        int j = (int) Math.floor(m * frac);
        if (j < 0) j = 0;
        if (j >= m) j = m - 1;
        return j;
    }

    private long totalCount() {
        long s = zeroOrNeg; // bunlar >t sayımlarında dikkate alınmaz
        for (long v : totalShards) s += v;
        return s;
    }

    /* ===================== Demo ===================== */

    /**
     * İnsan okuyabilir dump (debug)
     */
    public void printDebug() {
        System.out.println("=== ACTIVE ===");
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
        System.out.println("total=" + totalCount() + " (zeroOrNeg=" + zeroOrNeg + ")");
    }
}
