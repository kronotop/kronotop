/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The RoundRobin class provides a thread-safe mechanism for iterating over a collection of elements in a round-robin fashion.
 * It ensures that elements are returned in a cyclical manner, allowing for concurrent access and modifications.
 *
 * @param <T> the type of elements maintained by this RoundRobin instance.
 */
public class RoundRobin<T> {
    private final List<T> elements;
    private final ReentrantLock lock = new ReentrantLock(true);
    private int offset;

    public RoundRobin(List<T> elements) {
        this.elements = new ArrayList<>(elements);
    }

    /**
     * Retrieves the next element in the collection in a round-robin fashion.
     * This method is thread-safe and ensures that elements are accessed cyclically.
     *
     * @return the next element of type T in the sequence.
     * @throws IllegalStateException if the collection is empty or if the offset state becomes invalid.
     */
    public T next() {
        lock.lock();
        try {
            if (elements.isEmpty()) {
                throw new IllegalStateException("No more elements available in the round-robin scheduler");
            }

            if (offset >= elements.size()) {
                offset %= elements.size();
            }

            if (offset >= elements.size()) {
                throw new IllegalStateException("Corrupted internal state, offset is out of bounds");
            }

            T element = elements.get(offset);
            offset++;
            return element;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds a new element to the collection. This method is thread-safe and ensures
     * that duplicate elements cannot be added. If the element already exists in the
     * collection, an exception will be thrown.
     *
     * @param element the element of type T to be added to the collection.
     * @throws IllegalStateException if the element already exists in the collection.
     */
    public void add(T element) {
        lock.lock();
        try {
            if (elements.contains(element)) {
                throw new IllegalStateException("Element already exists");
            }
            elements.add(element);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the specified element from the collection. This operation is thread-safe.
     *
     * @param element the element of type T to be removed from the collection.
     */
    public void remove(T element) {
        lock.lock();
        try {
            elements.remove(element);
        } finally {
            lock.unlock();
        }
    }
}
