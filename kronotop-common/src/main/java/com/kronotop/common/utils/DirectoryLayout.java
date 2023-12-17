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

package com.kronotop.common.utils;

import java.util.ArrayList;
import java.util.List;

public class DirectoryLayout {
    public static String ROOT_DIRECTORY = "kronotop";
    private final List<String> items = new ArrayList<>(List.of(ROOT_DIRECTORY));

    public DirectoryLayout clusterName(String clusterName) {
        items.add(clusterName);
        return this;
    }

    public DirectoryLayout logicalDatabase(String logicalDatabase) {
        items.add(logicalDatabase);
        return this;
    }

    public DirectoryLayout shardId(String shardId) {
        items.add(shardId);
        return this;
    }

    public DirectoryLayout internal() {
        items.add("internal");
        return this;
    }

    public DirectoryLayout redis() {
        items.add("redis");
        return this;
    }

    public DirectoryLayout dataStructure(String dataStructure) {
        items.add(dataStructure);
        return this;
    }

    public DirectoryLayout persistence() {
        items.add("persistence");
        return this;
    }

    public DirectoryLayout namespaces() {
        items.add("namespaces");
        return this;
    }

    public DirectoryLayout zmap() {
        items.add("zmap");
        return this;
    }

    public DirectoryLayout cluster() {
        items.add("cluster");
        return this;
    }

    public DirectoryLayout memberlist() {
        items.add("memberlist");
        return this;
    }

    public DirectoryLayout shards() {
        items.add("shards");
        return this;
    }

    public DirectoryLayout journals() {
        items.add("journals");
        return this;
    }

    public DirectoryLayout addAll(List<String> list) {
        items.addAll(list);
        return this;
    }

    @Override
    public String toString() {
        return String.join(".", items);
    }

    public List<String> asList() {
        return items;
    }

    public static class Builder {
        public static DirectoryLayout clusterName(String clusterName) {
            return new DirectoryLayout().clusterName(clusterName);
        }

        public static DirectoryLayout logicalDatabase(String logicalDatabase) {
            return new DirectoryLayout().logicalDatabase(logicalDatabase);
        }

        public static DirectoryLayout shardId(String shardId) {
            return new DirectoryLayout().shardId(shardId);
        }

        public static DirectoryLayout internal() {
            return new DirectoryLayout().internal();
        }

        public static DirectoryLayout redis() {
            return new DirectoryLayout().redis();
        }

        public static DirectoryLayout dataStructure(String dataStructure) {
            return new DirectoryLayout().dataStructure(dataStructure);
        }

        public static DirectoryLayout persistence() {
            return new DirectoryLayout().persistence();
        }

        public static DirectoryLayout namespaces() {
            return new DirectoryLayout().namespaces();
        }

        public static DirectoryLayout cluster() {
            return new DirectoryLayout().cluster();
        }

        public static DirectoryLayout memberlist() {
            return new DirectoryLayout().memberlist();
        }

        public static DirectoryLayout shards() {
            return new DirectoryLayout().shards();
        }

        public static DirectoryLayout journals() {
            return new DirectoryLayout().journals();
        }

        public static DirectoryLayout addAll(List<String> list) {
            return new DirectoryLayout().addAll(list);
        }
    }
}