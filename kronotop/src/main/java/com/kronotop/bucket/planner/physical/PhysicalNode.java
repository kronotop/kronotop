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

package com.kronotop.bucket.planner.physical;

import java.util.ArrayList;
import java.util.List;

public class PhysicalNode {
    protected final List<PhysicalNode> children;

    public PhysicalNode() {
        this.children = new ArrayList<>();
    }

    public PhysicalNode(List<PhysicalNode> children) {
        this.children = children;
    }

    void addChild(PhysicalNode child) {
        children.add(child);
    }

    public List<PhysicalNode> getChildren() {
        return children;
    }

    void setChildren(List<PhysicalNode> children) {
        this.children.clear();
        this.children.addAll(children);
    }
}
