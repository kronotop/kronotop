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

package com.kronotop.server;


public enum RESPError {
    NOAUTH,
    WRONGPASS,
    TRANSACTION,
    TRANSACTIONOLD,
    NAMESPACEALREADYEXISTS,
    NOSUCHNAMESPACE,
    NOSUCHINDEX,
    NOSUCHBUCKET,
    WRONGTYPE,
    EXECABORT,
    MOVED,
    CROSSSLOT,
    NOPROTO,
    ERR;

    public final static String TRANSACTION_TOO_OLD_MESSAGE = "Transaction is too old to perform reads or be committed";
    public final static String TRANSACTION_BYTE_LIMIT_MESSAGE = "Transaction exceeds byte limit";
    public final static String WRONGTYPE_MESSAGE = "Operation against a key holding the wrong kind of value";
    public final static String NUMBER_FORMAT_EXCEPTION_MESSAGE_LONG = "value is not a long or out of range";
    public final static String NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER = "value is not an integer or out of range";
    public final static String NUMBER_FORMAT_EXCEPTION_MESSAGE_FLOAT = "value is not a valid float";
    public final static String EXECABORT_MESSAGE = "Transaction discarded because of previous errors.";
    public final static String PROTOCOL_VERSION_FORMAT_ERROR = "Protocol version is not an integer or out of range";
    public final static String CROSSSLOT_MESSAGE = "Keys in request don't hash to the same slot";
    public final static String UNSUPPORTED_PROTOCOL_VERSION = "unsupported protocol version";

    public static String decapitalize(String string) {
        if (string == null || string.isEmpty()) {
            return string;
        }

        char[] c = string.toCharArray();
        c[0] = Character.toLowerCase(c[0]);

        return new String(c);
    }

    public String toString() {
        return this.name();
    }
}
