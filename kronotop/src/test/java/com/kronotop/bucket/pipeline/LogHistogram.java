package com.kronotop.bucket.pipeline;

public class LogHistogram {
    private final int m;       // her dekad için alt bucket sayısı
    private final int dMin;    // minimum dekad
    private final int dMax;    // maksimum dekad
    private final long[][] counts; // [dekad-dMin][m] sayaçlar
    private long total = 0;

    public LogHistogram(int m, int dMin, int dMax) {
        this.m = m;
        this.dMin = dMin;
        this.dMax = dMax;
        this.counts = new long[dMax - dMin + 1][m];
    }

    // Demo
    public static void main(String[] args) {
        LogHistogram hist = new LogHistogram(8, 1, 5); // m=8, dekad 10^1..10^5
        double[] values = {30, 40, 250, 2587, 4589, 99, 123, 999, 10000};
        for (double v : values) hist.add(v);

        hist.print();

        System.out.println("Selectivity price>25: " + hist.estimateGreaterThan(25));
        System.out.println("Selectivity price>200: " + hist.estimateGreaterThan(200));
        System.out.println("Selectivity price>3000: " + hist.estimateGreaterThan(3000));
    }

    /**
     * Değeri uygun bucketa ekle
     */
    public void add(double value) {
        if (value <= 0) return;
        double log = Math.log10(value);
        int d = (int) Math.floor(log);
        if (d < dMin || d > dMax) return; // under/overflow
        double frac = log - d;
        int j = (int) Math.floor(m * frac);
        if (j == m) j = m - 1;
        counts[d - dMin][j]++;
        total++;
    }

    /**
     * Bucket sınırlarını hesapla: [L,U)
     */
    public double[] bucketBounds(int d, int j) {
        double lower = Math.pow(10, d + (double) j / m);
        double upper = Math.pow(10, d + (double) (j + 1) / m);
        return new double[]{lower, upper};
    }

    /**
     * price > t seçicilik tahmini
     */
    public double estimateGreaterThan(double t) {
        if (total == 0) return 0.0;
        if (t <= 0) return 1.0;

        double log = Math.log10(t);
        int d = (int) Math.floor(log);
        if (d < dMin) return 1.0;          // her şey daha büyük
        if (d > dMax) return 0.0;          // hiçbir şey daha büyük değil

        double frac = log - d;
        int j = (int) Math.floor(m * frac);
        if (j == m) j = m - 1;

        double[] bounds = bucketBounds(d, j);
        double ratio = 0.0;
        if (bounds[1] > bounds[0]) {
            ratio = (Math.log10(bounds[1]) - Math.log10(t)) /
                    (Math.log10(bounds[1]) - Math.log10(bounds[0]));
            if (ratio < 0) ratio = 0;
            if (ratio > 1) ratio = 1;
        }

        long countAbove = 0;
        // aynı dekad içindeki j'den büyük bucketlar
        for (int jj = j + 1; jj < m; jj++) {
            countAbove += counts[d - dMin][jj];
        }
        // daha büyük dekadlar
        for (int di = d - dMin + 1; di < counts.length; di++) {
            for (int jj = 0; jj < m; jj++) {
                countAbove += counts[di][jj];
            }
        }

        // kısmi bucket katkısı
        countAbove += (long) (counts[d - dMin][j] * ratio);

        return (double) countAbove / total;
    }

    /**
     * Histogramı yazdır
     */
    public void print() {
        for (int di = 0; di < counts.length; di++) {
            int d = dMin + di;
            for (int j = 0; j < m; j++) {
                long c = counts[di][j];
                if (c > 0) {
                    double[] b = bucketBounds(d, j);
                    System.out.printf("Bucket d=%d j=%d [%.2f, %.2f): %d\n",
                            d, j, b[0], b[1], c);
                }
            }
        }
    }
}
