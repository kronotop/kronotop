/*
 * Copyright (c) 2023 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.sql.executor;

import java.util.HashMap;

/**
 * The Row class represents a row of data with field-value pairs.
 */
public class Row<T> {
    private final HashMap<String, T> fields = new HashMap<>();
    private final HashMap<Integer, String> indexes = new HashMap<>();

    public T put(String field, Integer index, T value) {
        fields.put(field, value);
        indexes.put(index, field);
        return value;
    }

    public T rename(String old, String field, Integer index) {
        T value = fields.remove(old);
        return put(field, index, value);
    }

    public boolean hasField(String field) {
        return fields.containsKey(field);
    }

    public T remove(String field) {
        T value = fields.remove(field);
        indexes.entrySet().removeIf(entry -> entry.getValue().equals(field));
        return value;
    }

    public T get(String field) {
        return fields.get(field);
    }

    public T getByIndex(Integer index) {
        String field = indexes.get(index);
        if (field == null) {
            return null;
        }
        return fields.get(field);
    }
}
