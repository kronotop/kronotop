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
    EXECABORT,
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

    /**
     * Extracts RESP error information from an FDBException.
     *
     * @param fdbEx the FoundationDB exception
     * @return a record containing the error prefix and message
     */
    public static FDBErrorResult extractFDBError(FDBException fdbEx) {
        FDBErrorInfo errInfo = FDBErrorRegistry.lookup(fdbEx.getCode());
        if (errInfo == null) {
            return new FDBErrorResult(ERR.name(), decapitalize(fdbEx.getMessage()));
        }
        return new FDBErrorResult(errInfo.prefix(), errInfo.message());
    }

    /**
     * Returns the cause that best describes an error for the client.
     * <p>
     * Third party exceptions often carry long internal messages that mean nothing to a client.
     * When such an exception is wrapped in a {@link KronotopException}, the wrapper holds the
     * message worth reporting, so the wrapper is returned instead. The root cause is returned
     * when the chain has no {@link KronotopException}.
     *
     * @param t the throwable to inspect
     * @return the outermost {@link KronotopException} below {@code t}, or the root cause if there is none
     */
    public static Throwable preferKronotopCause(Throwable t) {
        Throwable result = t;
        while (result.getCause() != null && result.getCause() != result) {
            result = result.getCause();
            if (result instanceof KronotopException) {
                break;
            }
        }
        return result;
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
