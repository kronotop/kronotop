/*
 * Copyright (c) 2023-2024 Kronotop
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

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "notes",
        "flags",
        "begin_search",
        "find_keys"
})
public class KeySpec {

    @JsonProperty("notes")
    private String notes;
    @JsonProperty("flags")
    private List<String> flags = new ArrayList<String>();
    @JsonProperty("begin_search")
    private BeginSearch beginSearch;
    @JsonProperty("find_keys")
    private FindKeys findKeys;

    @JsonProperty("notes")
    public String getNotes() {
        return notes;
    }

    @JsonProperty("notes")
    public void setNotes(String notes) {
        this.notes = notes;
    }

    @JsonProperty("flags")
    public List<String> getFlags() {
        return flags;
    }

    @JsonProperty("flags")
    public void setFlags(List<String> flags) {
        this.flags = flags;
    }

    @JsonProperty("begin_search")
    public BeginSearch getBeginSearch() {
        return beginSearch;
    }

    @JsonProperty("begin_search")
    public void setBeginSearch(BeginSearch beginSearch) {
        this.beginSearch = beginSearch;
    }

    @JsonProperty("find_keys")
    public FindKeys getFindKeys() {
        return findKeys;
    }

    @JsonProperty("find_keys")
    public void setFindKeys(FindKeys findKeys) {
        this.findKeys = findKeys;
    }

}
