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

package com.kronotop.core.cluster;

import com.apple.foundationdb.tuple.Versionstamp;

public class MemberJoinEvent extends MemberEvent {
    private MemberJoinEvent() {
        super();
    }

    public MemberJoinEvent(String host, int port, Versionstamp processID, long createdAt) {
        super(EventTypes.MEMBER_JOIN, host, port, processID, createdAt);
    }

    public MemberJoinEvent(String host, int port, Versionstamp processID) {
        super(EventTypes.MEMBER_JOIN, host, port, processID);
    }
}
