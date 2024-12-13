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

package com.kronotop.directory;

import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.common.KronotopException;

import java.util.List;

public class Members extends KronotopDirectoryNode {

    public Members(List<String> layout) {
        super(layout);
        layout.add("members");
    }

    public Member member(String memberId) {
        if (!MemberIdGenerator.validateId(memberId)) {
            throw new KronotopException("Invalid member ID: " + memberId);
        }
        return new Member(layout, memberId);
    }

    public static class Member extends KronotopDirectoryNode {
        public Member(List<String> layout, String memberId) {
            super(layout);
            layout.add(memberId);
        }
    }
}
