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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;

import javax.annotation.Nonnull;

public class Session {
    private final UserVersion userVersion = new UserVersion();
    private final Transaction transaction;
    private final Prefix prefix;

    public Session(@Nonnull Prefix prefix) {
        this.prefix = prefix;
        this.transaction = null;
    }

    public Session(@Nonnull Transaction tr, @Nonnull Prefix prefix) {
        this.transaction = tr;
        this.prefix = prefix;
    }

    public Prefix prefix() {
        return prefix;
    }

    public Transaction transaction() {
        return transaction;
    }

    protected int getAndIncrementUserVersion() {
        return userVersion.getAndIncrement();
    }
}
