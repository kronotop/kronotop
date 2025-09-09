package com.kronotop.bucket.pipeline;

import org.bson.BsonValue;

import java.util.*;

public final class UpdateOptions {
    private final Map<String, BsonValue> setOps;
    private final Set<String> unsetOps;
    private final Map<String, Number> incOps;
    private final boolean upsert;
    private final boolean multi;

    private UpdateOptions(Builder b) {
        this.setOps = Collections.unmodifiableMap(new LinkedHashMap<>(b.setOps));
        this.unsetOps = Collections.unmodifiableSet(new LinkedHashSet<>(b.unsetOps));
        this.incOps = Collections.unmodifiableMap(new LinkedHashMap<>(b.incOps));
        this.upsert = b.upsert;
        this.multi = b.multi;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, BsonValue> setOps() {
        return setOps;
    }

    public Set<String> unsetOps() {
        return unsetOps;
    }

    public Map<String, Number> incOps() {
        return incOps;
    }

    public boolean upsert() {
        return upsert;
    }

    public boolean multi() {
        return multi;
    }

    public boolean isEmpty() {
        return setOps.isEmpty() && unsetOps.isEmpty() && incOps.isEmpty();
    }

    public static final class Builder {
        private final Map<String, BsonValue> setOps = new LinkedHashMap<>();
        private final Set<String> unsetOps = new LinkedHashSet<>();
        private final Map<String, Number> incOps = new LinkedHashMap<>();
        private boolean upsert;
        private boolean multi;

        private static void requireField(String f) {
            if (f == null || f.isBlank()) {
                throw new IllegalArgumentException("field is null/blank");
            }
        }

        // --- fluent ops ---
        public Builder set(String field, BsonValue value) {
            requireField(field);
            setOps.put(field, value);
            return this;
        }

        public Builder unset(String field) {
            requireField(field);
            unsetOps.add(field);
            return this;
        }

        public Builder inc(String field, Number delta) {
            requireField(field);
            if (delta == null) throw new IllegalArgumentException("delta == null");
            incOps.put(field, delta);
            return this;
        }

        // --- bulk helpers ---
        public Builder putAllSet(Map<String, BsonValue> m) {
            if (m != null) m.forEach(this::set);
            return this;
        }

        public Builder addAllUnset(Collection<String> fields) {
            if (fields != null) fields.forEach(this::unset);
            return this;
        }

        public Builder putAllInc(Map<String, ? extends Number> m) {
            if (m != null) m.forEach(this::inc);
            return this;
        }

        // --- options ---
        public Builder upsert(boolean v) {
            this.upsert = v;
            return this;
        }

        public Builder multi(boolean v) {
            this.multi = v;
            return this;
        }

        // --- build ---
        public UpdateOptions build() {
            if (!unsetOps.isEmpty()) {
                unsetOps.forEach(setOps::remove);
                unsetOps.forEach(incOps::remove);
            }
            return new UpdateOptions(this);
        }
    }
}
