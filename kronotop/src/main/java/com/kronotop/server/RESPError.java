/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.FDBException;
import com.kronotop.KronotopException;
import com.kronotop.internal.FDBErrorInfo;
import com.kronotop.internal.FDBErrorRegistry;

public enum RESPError {
    NOAUTH,
    WRONGPASS,
    TRANSACTION,
    NAMESPACEALREADYEXISTS,
    NOSUCHNAMESPACE,
    NAMESPACEBEINGREMOVED,
    NOSUCHINDEX,
    BUCKETALREADYEXISTS,
    NOSUCHBUCKET,
    BUCKETBEINGREMOVED,
    WRONGTYPE,
    MOVED,
    CROSSSLOT,
    NOPROTO,
    REJECT,
    OUTOFBOUND,
    INDEXTYPE_MISMATCH,
    DUPLICATEKEY,
    BARRIERNOTSATISFIED,
    VECTORINDEXNOTREADY,
    ERR;

    public final static String WRONGTYPE_MESSAGE = "Operation against a key holding the wrong kind of value";
    public final static String PROTOCOL_VERSION_FORMAT_ERROR = "Protocol version is not an integer or out of range";
    // The messages below belong to commands that Redis also has, so they keep the Redis wording.
    public final static String UNSUPPORTED_PROTOCOL_VERSION = "unsupported protocol version";
    public final static String SYNTAX_ERROR_MESSAGE = "syntax error";
    public final static String WRONGPASS_MESSAGE = "invalid username-password pair or user is disabled.";
    public final static String HELLO_NOAUTH_MESSAGE = "HELLO must be called with the client already authenticated, " +
            "otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and " +
            "select the RESP protocol version at the same time";
    public final static String CLIENT_NAME_SUBJECT = "Client names";
    public final static String CLIENT_ATTRIBUTE_FORMAT_ERROR = "%s cannot contain spaces, newlines or special characters.";

    /**
     * Extracts RESP error information from an FDBException.
     *
     * @param fdbEx the FoundationDB exception
     * @return a record containing the error prefix and message
     */
    public static FDBErrorResult extractFDBError(FDBException fdbEx) {
        FDBErrorInfo errInfo = FDBErrorRegistry.lookup(fdbEx.getCode());
        if (errInfo == null) {
            return new FDBErrorResult(ERR.name(), fdbEx.getMessage());
        }
        return new FDBErrorResult(errInfo.prefix(), errInfo.message());
    }

    /**
     * Returns the throwable in a cause chain whose message is reported to the client.
     *
     * @param t the throwable to inspect
     * @return the outermost {@link KronotopException} that has its own message, or the root cause if there is none
     */
    public static Throwable preferKronotopCause(Throwable t) {
        Throwable result = t;
        while (!hasOwnMessage(result) && result.getCause() != null && result.getCause() != result) {
            result = result.getCause();
        }
        return result;
    }

    // A KronotopException built from a cause alone only repeats the cause, class name included.
    private static boolean hasOwnMessage(Throwable t) {
        return t instanceof KronotopException kr && !kr.isCauseOnly();
    }

    /**
     * Returns a message that is always safe to report to a client.
     * <p>
     * An exception thrown with no message would leave the client with nothing to read, so the
     * exception type name is used instead. It says less than a real message, but it still tells
     * the client what went wrong.
     *
     * @param t the throwable to read the message from
     * @return the message of {@code t}, or its simple class name when the message is null
     */
    public static String getMessage(Throwable t) {
        return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    }

    public String toString() {
        return this.name();
    }

    public record FDBErrorResult(String prefix, String message) {
    }
}
