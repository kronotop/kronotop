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

package com.kronotop.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Route {
    private final Member primary;
    private final List<Member> standbys = new ArrayList<>();

    public Route(Member primary) {
        this.primary = primary;
    }

    public Route(Member primary, List<Member> standbys) {
        this(primary);
        this.standbys.addAll(standbys);
    }

    public Member getPrimary() {
        return primary;
    }

    public List<Member> getStandbys() {
        return Collections.unmodifiableList(standbys);
    }
}
