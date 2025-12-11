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

package com.kronotop.volume.changelog;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.OperationKind;

import java.util.Optional;

public class ChangeLogEntry {

    private final OperationKind kind;

    private final Versionstamp versionstamp;

    // Nullable: Sadece UPDATE ve DELETE islemlerinde doludur.
    // APPEND islemi icin null doner.
    private final ChangeLogCoordinate before;

    // Nullable: Sadece APPEND ve UPDATE islemlerinde doludur.
    // DELETE islemi icin null doner.
    private final ChangeLogCoordinate after;

    private final long prefix;

    // Internal Constructor (Sadece senin Iterator'in kullanacak)
    ChangeLogEntry(Versionstamp versionstamp, OperationKind kind, ChangeLogCoordinate before, ChangeLogCoordinate after, long prefix) {
        this.versionstamp = versionstamp;
        this.kind = kind;
        this.before = before;
        this.after = after;
        this.prefix = prefix;
    }

    // --- Public Interface ---

    /**
     * İşlemden önceki verinin koordinatlarını döner.
     *
     * @return DELETE veya UPDATE ise koordinat, APPEND ise Optional.empty()
     */
    public Optional<ChangeLogCoordinate> getBefore() {
        return Optional.ofNullable(before);
    }

    /**
     * İşlemden sonraki verinin koordinatlarını döner.
     *
     * @return APPEND veya UPDATE ise koordinat, DELETE ise Optional.empty()
     */
    public Optional<ChangeLogCoordinate> getAfter() {
        return Optional.ofNullable(after);
    }

    /**
     * Bu kayda ait prefix (Namespace/Tenant ID vb.)
     */
    public long getPrefix() {
        return prefix;
    }

    public Versionstamp getVersionstamp() {
        return versionstamp;
    }

    // --- Convenience Methods (İsteğe Bağlı) ---

    // Tüketici null check ile uğraşmak istemiyorsa:
    public boolean hasBefore() {
        return before != null;
    }

    public boolean hasAfter() {
        return after != null;
    }

    public OperationKind getKind() {
        return kind;
    }
}