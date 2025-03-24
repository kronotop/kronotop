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

package com.kronotop.server.handlers.protocol;

import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.common.KronotopException;
import com.kronotop.internal.ByteBufUtils;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

public class SessionAttributeParameters {
    private final SessionAttributeSubcommand subcommand;
    private SessionAttribute attribute;
    private Boolean futures;

    public SessionAttributeParameters(ArrayList<ByteBuf> params) {
        String rawSubcommand = ByteBufUtils.readAsString(params.get(0));
        try {
            subcommand = SessionAttributeSubcommand.valueOf(rawSubcommand.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid subcommand status: " + rawSubcommand);
        }

        if (subcommand.equals(SessionAttributeSubcommand.LIST)) {
            return;
        }

        if (subcommand.equals(SessionAttributeSubcommand.SET)) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }
        }

        if (subcommand.equals(SessionAttributeSubcommand.UNSET)) {
            if (params.size() != 2) {
                throw new InvalidNumberOfParametersException();
            }
        }

        String rawSessionAttribute = ByteBufUtils.readAsString(params.get(1));
        try {
            attribute = SessionAttribute.valueOf(rawSessionAttribute.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid session attribute: " + rawSessionAttribute);
        }

        switch (attribute) {
            case FUTURES:
                futures = ByteBufUtils.readBooleanValue(params.get(2));
                break;
            default:
                throw new KronotopException("Unknown session attribute: " + rawSessionAttribute);
        }
    }

    public SessionAttributeSubcommand getSubcommand() {
        return subcommand;
    }

    public SessionAttribute getAttribute() {
        return attribute;
    }

    public Boolean getFutures() {
        return futures;
    }

    public enum SessionAttributeSubcommand {
        SET,
        UNSET,
        LIST
    }

    public enum SessionAttribute {
        FUTURES
    }
}