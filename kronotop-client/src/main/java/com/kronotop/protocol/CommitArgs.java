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

package com.kronotop.protocol;

import io.lettuce.core.protocol.CommandArgs;

/**
 * Represents the arguments for a COMMIT operation.
 */
public class CommitArgs {
    private CommitKeyword[] keywords;

    public CommitArgs returning(CommitKeyword... keywords) {
        this.keywords = keywords;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (keywords != null) {
            args.add("RETURNING");
            for (CommitKeyword keyword : keywords) {
                args.add(keyword);
            }
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static CommitArgs returning(CommitKeyword... keywords) {
            return new CommitArgs().returning(keywords);
        }
    }
}