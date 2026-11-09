/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 *
 * This file contains contributions from third-party contributors
 * licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kronotop.commands.redis;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.GenericMapOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueListOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.BaseRedisCommandBuilder;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;
import java.util.Map;

import static io.lettuce.core.protocol.CommandKeyword.SETINFO;
import static io.lettuce.core.protocol.CommandKeyword.SETNAME;
import static io.lettuce.core.protocol.CommandType.*;

// Source: https://raw.githubusercontent.com/redis/lettuce/a2d070e0028350ced3200bc149ddcb915768b5f7/src/main/java/io/lettuce/core/RedisCommandBuilder.java

/**
 * @param <K>
 * @param <V>
 * @author Mark Paluch
 * @author Zhang Jessey
 * @author Tugdual Grall
 * @author dengliming
 * @author Mikhael Sokolov
 * @author Tihomir Mateev
 * @author Ali Takavci
 * @author Seonghwan Lee
 */
@SuppressWarnings({"unchecked", "varargs"})
public class RedisCommandBuilder<K, V> extends BaseRedisCommandBuilder<K, V> {

    public RedisCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    public Command<K, V, String> auth(CharSequence password) {
        LettuceAssert.notNull(password, "Password " + MUST_NOT_BE_NULL);

        char[] chars = new char[password.length()];
        for (int i = 0; i < password.length(); i++) {
            chars[i] = password.charAt(i);
        }
        return auth(chars);
    }

    public Command<K, V, String> auth(char[] password) {
        LettuceAssert.notNull(password, "Password " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(password);
        return createCommand(AUTH, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> auth(String username, CharSequence password) {
        LettuceAssert.notNull(username, "Username " + MUST_NOT_BE_NULL);
        LettuceAssert.isTrue(!username.isEmpty(), "Username " + MUST_NOT_BE_EMPTY);
        LettuceAssert.notNull(password, "Password " + MUST_NOT_BE_NULL);

        char[] chars = new char[password.length()];
        for (int i = 0; i < password.length(); i++) {
            chars[i] = password.charAt(i);
        }
        return auth(username, chars);
    }

    public Command<K, V, String> auth(String username, char[] password) {
        LettuceAssert.notNull(username, "Username " + MUST_NOT_BE_NULL);
        LettuceAssert.isTrue(!username.isEmpty(), "Username " + MUST_NOT_BE_EMPTY);
        LettuceAssert.notNull(password, "Password " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(username).add(password);
        return createCommand(AUTH, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> clientSetname(K name) {
        LettuceAssert.notNull(name, "Name " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SETNAME).addKey(name);
        return createCommand(CLIENT, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> clientSetinfo(String key, String value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SETINFO).add(key).add(value);
        return createCommand(CLIENT, new StatusOutput<>(codec), args);
    }

    public Command<K, V, V> echo(V msg) {
        LettuceAssert.notNull(msg, "message " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addValue(msg);
        return createCommand(ECHO, new ValueOutput<>(codec), args);
    }

    public Command<String, String, Map<String, Object>> hello(int protocolVersion, String user, char[] password, String name) {

        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.ASCII).add(protocolVersion);

        if (user != null && password != null) {
            args.add(AUTH).add(user).add(password);
        }

        if (name != null) {
            args.add(SETNAME).add(name);
        }

        return new Command<>(HELLO, new GenericMapOutput<>(StringCodec.ASCII), args);
    }

    public Command<K, V, String> ping() {
        return createCommand(PING, new StatusOutput<>(codec));
    }

    public Command<K, V, List<V>> time() {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        return createCommand(TIME, new ValueListOutput<>(codec), args);
    }
}
