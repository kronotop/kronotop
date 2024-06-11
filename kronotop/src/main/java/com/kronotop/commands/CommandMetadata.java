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
        "summary",
        "complexity",
        "group",
        "since",
        "arity",
        "function",
        "get_keys_function",
        "history",
        "command_flags",
        "acl_categories",
        "command_tips",
        "key_specs",
        "arguments"
})
public class CommandMetadata {
    @JsonProperty("summary")
    private String summary;

    @JsonProperty("complexity")
    private String complexity;

    @JsonProperty("group")
    private String group;

    @JsonProperty("since")
    private String since;

    @JsonProperty("arity")
    private int arity;

    @JsonProperty("function")
    private String function;

    @JsonProperty("get_keys_function")
    private String getKeysFunction;

    @JsonProperty("history")
    private List<List<String>> history = new ArrayList<>();

    @JsonProperty("command_flags")
    private List<String> commandFlags = new ArrayList<>();

    @JsonProperty("acl_categories")
    private List<String> aclCategories = new ArrayList<>();

    @JsonProperty("command_tips")
    private List<String> commandTips = new ArrayList<>();

    @JsonProperty("key_specs")
    private List<KeySpec> keySpecs = new ArrayList<>();

    @JsonProperty("arguments")
    private List<Argument> arguments = new ArrayList<>();

    @JsonProperty("summary")
    public String getSummary() {
        return summary;
    }

    @JsonProperty("summary")
    public void setSummary(String summary) {
        this.summary = summary;
    }

    @JsonProperty("complexity")
    public String getComplexity() {
        return complexity;
    }

    @JsonProperty("complexity")
    public void setComplexity(String complexity) {
        this.complexity = complexity;
    }

    @JsonProperty("group")
    public String getGroup() {
        return group;
    }

    @JsonProperty("group")
    public void setGroup(String group) {
        this.group = group;
    }

    @JsonProperty("since")
    public String getSince() {
        return since;
    }

    @JsonProperty("since")
    public void setSince(String since) {
        this.since = since;
    }

    @JsonProperty("arity")
    public int getArity() {
        return arity;
    }

    @JsonProperty("arity")
    public void setArity(int arity) {
        this.arity = arity;
    }

    @JsonProperty("function")
    public String getFunction() {
        return function;
    }

    @JsonProperty("function")
    public void setFunction(String function) {
        this.function = function;
    }

    @JsonProperty("get_keys_function")
    public String getGetKeysFunction() {
        return getKeysFunction;
    }

    @JsonProperty("get_keys_function")
    public void setGetKeysFunction(String getKeysFunction) {
        this.getKeysFunction = getKeysFunction;
    }

    @JsonProperty("history")
    public List<List<String>> getHistory() {
        return history;
    }

    @JsonProperty("history")
    public void setHistory(List<List<String>> history) {
        this.history = history;
    }

    @JsonProperty("command_flags")
    public List<String> getCommandFlags() {
        return commandFlags;
    }

    @JsonProperty("command_flags")
    public void setCommandFlags(List<String> commandFlags) {
        this.commandFlags = commandFlags;
    }

    @JsonProperty("acl_categories")
    public List<String> getAclCategories() {
        return aclCategories;
    }

    @JsonProperty("acl_categories")
    public void setAclCategories(List<String> aclCategories) {
        this.aclCategories = aclCategories;
    }

    @JsonProperty("command_tips")
    public List<String> getCommandTips() {
        return commandTips;
    }

    @JsonProperty("command_tips")
    public void setCommandTips(List<String> commandTips) {
        this.commandTips = commandTips;
    }

    @JsonProperty("key_specs")
    public List<KeySpec> getKeySpecs() {
        return keySpecs;
    }

    @JsonProperty("key_specs")
    public void setKeySpecs(List<KeySpec> keySpecs) {
        this.keySpecs = keySpecs;
    }

    @JsonProperty("arguments")
    public List<Argument> getArguments() {
        return arguments;
    }

    @JsonProperty("arguments")
    public void setArguments(List<Argument> arguments) {
        this.arguments = arguments;
    }
}
