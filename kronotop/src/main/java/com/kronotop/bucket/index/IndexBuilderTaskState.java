/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.internal.task.TaskStorage;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public record IndexBuilderTaskState(Versionstamp cursorVersionstamp, Versionstamp highestVersionstamp,
                                    IndexTaskStatus status, String error) {
    public static final String CURSOR_VERSIONSTAMP = "cv";
    public static final String HIGHEST_VERSIONSTAMP = "hv";
    public static final String ERROR = "e";
    public static final String STATUS = "s";

    public static IndexBuilderTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);

        Versionstamp cursorVersionstamp = null;
        byte[] rawCursorVs = entries.get(CURSOR_VERSIONSTAMP);
        if (rawCursorVs != null) {
            cursorVersionstamp = Versionstamp.fromBytes(rawCursorVs);
        }

        Versionstamp highestVersionstamp = null;
        byte[] rawHighestVs = entries.get(HIGHEST_VERSIONSTAMP);
        if (rawHighestVs != null) {
            highestVersionstamp = Versionstamp.fromBytes(rawHighestVs);
        }

        String error = null;
        byte[] rawError = entries.get(ERROR);
        if (rawError != null) {
            error = new String(rawError, StandardCharsets.UTF_8);
        }

        IndexTaskStatus status = IndexTaskStatus.WAITING; // Initial status should be WAITING
        byte[] rawStatus = entries.get(STATUS);
        if (rawStatus != null) {
            status = IndexTaskStatus.valueOf(new String(rawStatus));
        }
        return new IndexBuilderTaskState(cursorVersionstamp, highestVersionstamp, status, error);
    }

    public static void setCursorVersionstamp(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, Versionstamp value) {
        TaskStorage.setStateField(tr, subspace, taskId, CURSOR_VERSIONSTAMP, value.getBytes());
    }

    public static void setHighestVersionstamp(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, Versionstamp value) {
        TaskStorage.setStateField(tr, subspace, taskId, HIGHEST_VERSIONSTAMP, value.getBytes());
    }

    public static void setError(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String error) {
        TaskStorage.setStateField(tr, subspace, taskId, ERROR, error.getBytes(StandardCharsets.UTF_8));
    }

    public static void setStatus(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, IndexTaskStatus status) {
        TaskStorage.setStateField(tr, subspace, taskId, STATUS, status.name().getBytes());
    }

    public static boolean isTerminal(IndexTaskStatus status) {
        return status.equals(IndexTaskStatus.COMPLETED) || status.equals(IndexTaskStatus.FAILED) || status.equals(IndexTaskStatus.STOPPED);
    }
}
