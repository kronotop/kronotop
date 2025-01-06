/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.protocol;

import io.lettuce.core.protocol.CommandArgs;

public class ZMutateArgs {
    private String mutationType;

    public ZMutateArgs add() {
        this.mutationType = ZMutationType.ADD.toString();
        return this;
    }

    public ZMutateArgs bitAnd() {
        this.mutationType = ZMutationType.BIT_AND.toString();
        return this;
    }

    public ZMutateArgs bitOr() {
        this.mutationType = ZMutationType.BIT_OR.toString();
        return this;
    }

    public ZMutateArgs bitXor() {
        this.mutationType = ZMutationType.BIT_XOR.toString();
        return this;
    }

    public ZMutateArgs appendIfFits() {
        this.mutationType = ZMutationType.APPEND_IF_FITS.toString();
        return this;
    }

    public ZMutateArgs max() {
        this.mutationType = ZMutationType.MAX.toString();
        return this;
    }

    public ZMutateArgs min() {
        this.mutationType = ZMutationType.MIN.toString();
        return this;
    }

    public ZMutateArgs setVersionStampedKey() {
        this.mutationType = ZMutationType.SET_VERSIONSTAMPED_KEY.toString();
        return this;
    }

    public ZMutateArgs setVersionStampedValue() {
        this.mutationType = ZMutationType.SET_VERSIONSTAMPED_VALUE.toString();
        return this;
    }

    public ZMutateArgs byteMin() {
        this.mutationType = ZMutationType.BYTE_MIN.toString();
        return this;
    }

    public ZMutateArgs byteMax() {
        this.mutationType = ZMutationType.BYTE_MAX.toString();
        return this;
    }

    public ZMutateArgs compareAndClear() {
        this.mutationType = ZMutationType.COMPARE_AND_CLEAR.toString();
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (mutationType == null) {
            throw new IllegalArgumentException("mutation type has to be set");
        }
        args.add(mutationType);
    }

    public static class Builder {
        private Builder() {
        }

        public static ZMutateArgs add() {
            return new ZMutateArgs().add();
        }

        public static ZMutateArgs bitAnd() {
            return new ZMutateArgs().bitAnd();
        }

        public static ZMutateArgs bitOr() {
            return new ZMutateArgs().bitOr();
        }

        public static ZMutateArgs bitXor() {
            return new ZMutateArgs().bitXor();
        }

        public static ZMutateArgs appendIfFits() {
            return new ZMutateArgs().appendIfFits();
        }

        public static ZMutateArgs max() {
            return new ZMutateArgs().max();
        }

        public static ZMutateArgs min() {
            return new ZMutateArgs().min();
        }

        public static ZMutateArgs setVersionStampedKey() {
            return new ZMutateArgs().setVersionStampedKey();
        }

        public static ZMutateArgs setVersionStampedValue() {
            return new ZMutateArgs().setVersionStampedValue();
        }

        public static ZMutateArgs byteMin() {
            return new ZMutateArgs().byteMin();
        }

        public static ZMutateArgs byteMax() {
            return new ZMutateArgs().byteMax();
        }

        public static ZMutateArgs compareAndClear() {
            return new ZMutateArgs().compareAndClear();
        }
    }
}
