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

package com.kronotop.bucket.pipeline;

public class Bounds {
    private volatile Bound lower;
    private volatile Bound upper;

    public Bounds(Bound lower, Bound upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public void setLower(Bound lower) {
        this.lower = lower;
    }

    public Bound getLower() {
        return lower;
    }

    public void setUpper(Bound upper) {
        this.upper = upper;
    }

    public Bound getUpper() {
        return upper;
    }
}
