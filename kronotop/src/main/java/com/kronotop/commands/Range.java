/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.commands;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "lastkey",
        "step",
        "limit"
})
public class Range {
    @JsonProperty("lastkey")
    private int lastkey;

    @JsonProperty("step")
    private int step;

    @JsonProperty("limit")
    private int limit;

    @JsonProperty("lastkey")
    public int getLastkey() {
        return lastkey;
    }

    @JsonProperty("lastkey")
    public void setLastkey(int lastkey) {
        this.lastkey = lastkey;
    }

    @JsonProperty("step")
    public int getStep() {
        return step;
    }

    @JsonProperty("step")
    public void setStep(int step) {
        this.step = step;
    }

    @JsonProperty("limit")
    public int getLimit() {
        return limit;
    }

    @JsonProperty("limit")
    public void setLimit(int limit) {
        this.limit = limit;
    }
}
