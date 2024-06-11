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

package com.kronotop.cluster.coordinator;

import com.kronotop.cluster.Member;

/**
 * The Route class represents a route in a routing table. It is associated with a Member object.
 */
public class Route {
    private Member member;

    Route() {
    }

    public Route(Member member) {
        this.member = member;
    }

    public Member getMember() {
        return member;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof Route)) {
            return false;
        }

        final Route route = (Route) obj;
        return route.getMember().equals(getMember());
    }
}
