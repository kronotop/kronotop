/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.changelog;

import com.kronotop.volume.ParentOperationKind;

public final class ChangeLogIterableOptions {
    private final SequenceNumberSelector begin;
    private final SequenceNumberSelector end;
    private final Integer limit;
    private final Boolean reverse;
    private final ParentOperationKind parentOperationKind;

    private ChangeLogIterableOptions(Builder b) {
        this.begin = b.begin;
        this.end = b.end;
        this.limit = b.limit;
        this.reverse = b.reverse;
        this.parentOperationKind = b.parentOperationKind;
    }

    public SequenceNumberSelector begin() {
        return begin;
    }

    public SequenceNumberSelector end() {
        return end;
    }

    public Integer limit() {
        return limit;
    }

    public Boolean reverse() {
        return reverse;
    }

    public ParentOperationKind parentOperationKind() {
        return parentOperationKind;
    }

    public static class Builder {
        private SequenceNumberSelector begin;
        private SequenceNumberSelector end;
        private Integer limit;
        private Boolean reverse;
        private ParentOperationKind parentOperationKind;

        public Builder begin(SequenceNumberSelector v) {
            this.begin = v;
            return this;
        }

        public Builder end(SequenceNumberSelector v) {
            this.end = v;
            return this;
        }

        public Builder limit(int v) {
            this.limit = v;
            return this;
        }

        public Builder reverse(boolean v) {
            this.reverse = v;
            return this;
        }

        public Builder parentOperationKind(ParentOperationKind v) {
            this.parentOperationKind = v;
            return this;
        }

        public ChangeLogIterableOptions build() {
            return new ChangeLogIterableOptions(this);
        }
    }
}
