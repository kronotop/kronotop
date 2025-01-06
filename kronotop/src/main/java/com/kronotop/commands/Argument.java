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

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "name",
        "type",
        "key_spec_index",
        "multiple",
        "optional",
        "since",
        "arguments",
        "token"
})
public class Argument {
    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private String type;

    @JsonProperty("key_spec_index")
    private int keySpecIndex;

    @JsonProperty("multiple")
    private boolean multiple;

    @JsonProperty("optional")
    private boolean optional;

    @JsonProperty("since")
    private String since;

    @JsonProperty("arguments")
    private List<Argument> arguments = new ArrayList<>();

    @JsonProperty("token")
    private String token;

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("key_spec_index")
    public int getKeySpecIndex() {
        return keySpecIndex;
    }

    @JsonProperty("key_spec_index")
    public void setKeySpecIndex(int keySpecIndex) {
        this.keySpecIndex = keySpecIndex;
    }

    @JsonProperty("multiple")
    public void setKeySpecIndex(boolean multiple) {
        this.multiple = multiple;
    }

    @JsonProperty("multiple")
    public boolean getMultiple() {
        return multiple;
    }

    @JsonProperty("optional")
    public boolean isOptional() {
        return optional;
    }

    @JsonProperty("optional")
    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    @JsonProperty("since")
    public String getSince() {
        return since;
    }

    @JsonProperty("since")
    public void setSince(String since) {
        this.since = since;
    }

    @JsonProperty("arguments")
    public List<Argument> getArguments() {
        return arguments;
    }

    @JsonProperty("arguments")
    public void setArguments(List<Argument> arguments) {
        this.arguments = arguments;
    }

    @JsonProperty("token")
    public String getToken() {
        return token;
    }

    @JsonProperty("token")
    public void setToken(String token) {
        this.token = token;
    }
}
