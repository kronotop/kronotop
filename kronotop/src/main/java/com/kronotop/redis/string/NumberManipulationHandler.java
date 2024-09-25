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

package com.kronotop.redis.string;

import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.RedisValueKind;

import java.util.function.Function;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

public class NumberManipulationHandler<T> {
    private final Function<T, byte[]> encode;
    private final Function<byte[], T> decode;

    public NumberManipulationHandler(Function<T, byte[]> encode, Function<byte[], T> decode) {
        this.encode = encode;
        this.decode = decode;
    }

    public static byte[] encodeInteger(Integer value) {
        return Integer.toString(value).getBytes();
    }

    public static Integer decodeInteger(byte[] value) {
        return Integer.parseInt(new String(value));
    }

    public static byte[] encodeDouble(Double value) {
        return Double.toString(value).getBytes();
    }

    public static Double decodeDouble(byte[] value) {
        return Double.parseDouble(new String(value));
    }

    public RedisValueContainer manipulate(RedisShard shard, String key, Function<T, T> logic) throws NumberFormatException {
        RedisValueContainer previous = shard.storage().get(key);
        T currentValue = null;
        if (previous != null) {
            checkRedisValueKind(previous, RedisValueKind.STRING);
            currentValue = decode.apply(previous.string().value());
        }

        currentValue = logic.apply(currentValue);
        StringValue value = new StringValue(encode.apply(currentValue));
        shard.storage().put(key, new RedisValueContainer(value));
        return previous;
    }
}
