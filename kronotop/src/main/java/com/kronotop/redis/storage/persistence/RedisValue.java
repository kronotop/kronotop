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

package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.tuple.Versionstamp;

/**
 * The RedisValue interface defines the contract for a value stored in Redis.
 * It provides methods to retrieve and set the value in byte array form,
 * as well as to retrieve and set the versionstamp for the value.
 */
public interface RedisValue {
    /**
     * Retrieves the value stored in the RedisValue as a byte array.
     *
     * @return the value as a byte array.
     */
    byte[] value();

    /**
     * Sets the value stored in the RedisValue as a byte array.
     *
     * @param value the new value to be set as a byte array.
     */
    void setValue(byte[] value);


    /**
     * Retrieves the versionstamp associated with the RedisValue.
     *
     * @return the versionstamp of the RedisValue.
     */
    Versionstamp versionstamp();


    /**
     * Sets the versionstamp associated with the RedisValue.
     *
     * @param versionstamp the new versionstamp to be set.
     */
    void setVersionstamp(Versionstamp versionstamp);
}
