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

package com.kronotop.cluster.client.protocol;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

import static io.lettuce.core.protocol.CommandType.PING;

public class InternalCommandBuilder<K, V> extends BaseInternalCommandBuilder<K, V> {
    public InternalCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    public Command<K, V, List<Object>> segmentrange(String volume, String segment, SegmentRange... ranges) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volume).add(segment);
        for (SegmentRange range : ranges) {
            args.add(range.position());
            args.add(range.length());
        }
        return createCommand(InternalCommandType.SEGMENTRANGE, new ArrayOutput<>(codec), args);
    }

    public Command<K, V, String> segmentinsert(String volume, String segment, PackedEntry... entries) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volume).add(segment);
        for (PackedEntry entry : entries) {
            args.add(entry.position());
            args.add(entry.data());
        }
        return createCommand(InternalCommandType.SEGMENTINSERT, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> ping() {
        return createCommand(PING, new StatusOutput<>(codec));
    }
}
