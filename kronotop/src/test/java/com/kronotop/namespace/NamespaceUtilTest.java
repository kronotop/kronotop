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

package com.kronotop.namespace;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseHandlerTest;
import com.kronotop.DataStructureKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.namespace.handlers.*;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NamespaceUtilTest extends BaseHandlerTest {

    @Test
    void shouldCreateNamespace() {
        String namespace = "test.namespace.create";
        NamespaceUtil.create(context, namespace);

        assertThat(NamespaceUtil.exists(context, List.of("test", "namespace", "create"))).isTrue();
    }

    @Test
    void shouldThrowExceptionWhenNamespaceAlreadyExists() {
        String namespace = "test.namespace.duplicate";
        NamespaceUtil.create(context, namespace);

        assertThatThrownBy(() -> NamespaceUtil.create(context, namespace))
                .isInstanceOf(NamespaceAlreadyExistsException.class);
    }

    @Test
    void shouldThrowExceptionWhenCreatingNamespaceBeingRemoved() {
        String namespace = "test.namespace.create.removed";
        NamespaceUtil.create(context, namespace);
        NamespaceUtil.setRemoved(context, namespace);

        assertThrows(NamespaceBeingRemovedException.class, () -> NamespaceUtil.create(context, namespace));
    }

    @Test
    void shouldReadMetadata() {
        String namespace = "test.namespace.metadata";
        NamespaceUtil.create(context, namespace);

        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);

        assertThat(metadata.name()).isEqualTo(namespace);
        assertThat(metadata.removed()).isFalse();
    }

    @Test
    void shouldThrowExceptionWhenReadingNonExistentNamespace() {
        assertThatThrownBy(() -> NamespaceUtil.readMetadata(context, "non.existent.namespace"))
                .isInstanceOf(NoSuchNamespaceException.class);
    }

    @Test
    void shouldSetRemoved() {
        String namespace = "test.namespace.removed";
        NamespaceUtil.create(context, namespace);

        NamespaceMetadata metadataBefore = NamespaceUtil.readMetadata(context, namespace);
        assertThat(metadataBefore.removed()).isFalse();

        NamespaceUtil.setRemoved(context, namespace);

        NamespaceMetadata metadataAfter = NamespaceUtil.readMetadata(context, namespace);
        assertThat(metadataAfter.removed()).isTrue();
        assertThat(metadataAfter.name()).isEqualTo(namespace);
        assertThat(metadataAfter.id()).isEqualTo(metadataBefore.id());
    }

    @Test
    void shouldIncrementVersionOnSetRemoved() {
        String namespace = "test.namespace.version";
        NamespaceUtil.create(context, namespace);

        NamespaceMetadata metadataBefore = NamespaceUtil.readMetadata(context, namespace);
        assertThat(metadataBefore.version()).isEqualTo(0);

        NamespaceUtil.setRemoved(context, namespace);

        NamespaceMetadata metadataAfter = NamespaceUtil.readMetadata(context, namespace);
        assertThat(metadataAfter.version()).isEqualTo(1);
    }

    @Test
    void shouldOpenDataStructureSubspace() {
        String namespace = "test.namespace.open";
        NamespaceUtil.create(context, namespace);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = NamespaceUtil.open(tr, context.getClusterName(), namespace, DataStructureKind.BUCKET);
            assertThat(subspace).isNotNull();
        }
    }

    @Test
    void shouldThrowExceptionWhenOpeningNonExistentNamespace() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThatThrownBy(() -> NamespaceUtil.open(tr, context.getClusterName(), "non.existent.namespace", DataStructureKind.BUCKET))
                    .isInstanceOf(NoSuchNamespaceException.class);
        }
    }

    @Test
    void shouldOpenDataStructureSubspaceWithSession() {
        String namespaceName = "test.namespace.session";
        NamespaceUtil.create(context, namespaceName);

        Session session = Session.extractSessionFromChannel(channel);
        session.attr(SessionAttributes.CURRENT_NAMESPACE).set(namespaceName);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = NamespaceUtil.openDataStructureSubspace(context, tr, session, DataStructureKind.BUCKET);
            assertThat(subspace).isNotNull();

            // Verify namespace is cached in session
            Map<String, Namespace> openNamespaces = session.attr(SessionAttributes.OPEN_NAMESPACES).get();
            assertThat(openNamespaces).containsKey(namespaceName);
            assertThat(openNamespaces.get(namespaceName).get(DataStructureKind.BUCKET)).isPresent();
        }
    }

    @Test
    void shouldReturnCachedSubspaceOnSubsequentCalls() {
        String namespaceName = "test.namespace.cached";
        NamespaceUtil.create(context, namespaceName);

        Session session = Session.extractSessionFromChannel(channel);
        session.attr(SessionAttributes.CURRENT_NAMESPACE).set(namespaceName);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace first = NamespaceUtil.openDataStructureSubspace(context, tr, session, DataStructureKind.BUCKET);
            DirectorySubspace second = NamespaceUtil.openDataStructureSubspace(context, tr, session, DataStructureKind.BUCKET);
            assertThat(first).isSameAs(second);
        }
    }

    @Test
    void shouldThrowExceptionWhenNamespaceNotSetInSession() {
        Session session = Session.extractSessionFromChannel(channel);
        session.attr(SessionAttributes.CURRENT_NAMESPACE).set(null);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThatThrownBy(() -> NamespaceUtil.openDataStructureSubspace(context, tr, session, DataStructureKind.BUCKET))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("namespace not specified");
        }
    }

    @Test
    void shouldRemoveNamespaceByName() {
        String namespace = "test.namespace.remove.name";
        NamespaceUtil.create(context, namespace);
        assertThat(NamespaceUtil.exists(context, List.of("test", "namespace", "remove", "name"))).isTrue();

        NamespaceUtil.remove(context, namespace);
        assertThat(NamespaceUtil.exists(context, List.of("test", "namespace", "remove", "name"))).isFalse();
    }

    @Test
    void shouldRemoveNamespaceByList() {
        String namespace = "test.namespace.remove.list";
        NamespaceUtil.create(context, namespace);
        assertThat(NamespaceUtil.exists(context, List.of("test", "namespace", "remove", "list"))).isTrue();

        NamespaceUtil.remove(context, List.of("test", "namespace", "remove", "list"));
        assertThat(NamespaceUtil.exists(context, List.of("test", "namespace", "remove", "list"))).isFalse();
    }

    @Test
    void shouldRemoveNamespaceWithTransaction() {
        String namespace = "test.namespace.remove.transaction";
        NamespaceUtil.create(context, namespace);
        assertThat(NamespaceUtil.exists(context, List.of("test", "namespace", "remove", "transaction"))).isTrue();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NamespaceUtil.remove(tr, context.getClusterName(), List.of("test", "namespace", "remove", "transaction"));
            tr.commit().join();
        }
        assertThat(NamespaceUtil.exists(context, List.of("test", "namespace", "remove", "transaction"))).isFalse();
    }

    @Test
    void shouldReturnTrueWhenNamespaceExists() {
        String namespace = "test.namespace.exists";
        NamespaceUtil.create(context, namespace);

        assertThat(NamespaceUtil.exists(context, List.of("test", "namespace", "exists"))).isTrue();
    }

    @Test
    void shouldReturnFalseWhenNamespaceDoesNotExist() {
        assertThat(NamespaceUtil.exists(context, List.of("non", "existent", "namespace"))).isFalse();
    }

    @Test
    void shouldThrowExceptionWhenNamespaceIsBeingRemoved() {
        String namespace = "test.namespace.being.removed";
        NamespaceUtil.create(context, namespace);
        NamespaceUtil.setRemoved(context, namespace);

        assertThatThrownBy(() -> NamespaceUtil.exists(context, List.of("test", "namespace", "being", "removed")))
                .isInstanceOf(NamespaceBeingRemovedException.class);
    }

    @Test
    void shouldThrowExceptionWhenParentNamespaceIsBeingRemoved() {
        // Create parent and child namespaces
        NamespaceUtil.create(context, "hierarchy.parent");
        NamespaceUtil.create(context, "hierarchy.parent.child");

        // Mark parent as removed
        NamespaceUtil.setRemoved(context, "hierarchy.parent");

        // Checking child should fail because parent is being removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThatThrownBy(() -> NamespaceUtil.checkBeingRemoved(tr, context.getClusterName(), List.of("hierarchy", "parent", "child")))
                    .isInstanceOf(NamespaceBeingRemovedException.class)
                    .hasMessageContaining("hierarchy.parent");
        }
    }

    @Test
    void shouldThrowExceptionWhenIntermediateNamespaceIsBeingRemoved() {
        // Create a.b.c.d hierarchy
        NamespaceUtil.create(context, "a.b");
        NamespaceUtil.create(context, "a.b.c");
        NamespaceUtil.create(context, "a.b.c.d");

        // Mark intermediate namespace "a.b.c" as removed
        NamespaceUtil.setRemoved(context, "a.b.c");

        // Checking "a.b.c.d" should fail because "a.b.c" is being removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThatThrownBy(() -> NamespaceUtil.checkBeingRemoved(tr, context.getClusterName(), List.of("a", "b", "c", "d")))
                    .isInstanceOf(NamespaceBeingRemovedException.class)
                    .hasMessageContaining("a.b.c");
        }
    }

    @Test
    void shouldThrowExceptionWhenGrandparentNamespaceIsBeingRemoved() {
        // Create a deep hierarchy
        NamespaceUtil.create(context, "deep.level1");
        NamespaceUtil.create(context, "deep.level1.level2");
        NamespaceUtil.create(context, "deep.level1.level2.level3");

        // Mark grandparent as removed
        NamespaceUtil.setRemoved(context, "deep.level1");

        // Checking deepest level should fail because grandparent is being removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThatThrownBy(() -> NamespaceUtil.checkBeingRemoved(tr, context.getClusterName(), List.of("deep", "level1", "level2", "level3")))
                    .isInstanceOf(NamespaceBeingRemovedException.class)
                    .hasMessageContaining("deep.level1");
        }
    }

    @Test
    void shouldNotThrowExceptionWhenNoNamespaceIsBeingRemoved() {
        // Create hierarchy without marking any as removed
        NamespaceUtil.create(context, "clean.hierarchy");
        NamespaceUtil.create(context, "clean.hierarchy.child");

        // Check should pass without exception
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NamespaceUtil.checkBeingRemoved(tr, context.getClusterName(), List.of("clean", "hierarchy", "child"));
        }
    }

    @Test
    void shouldSetAndReadLastSeenNamespaceVersion() {
        String namespace = "test.namespace.version.tracking";
        NamespaceUtil.create(context, namespace);

        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
        long namespaceId = metadata.id();

        // Increment version by calling setRemoved
        NamespaceUtil.setRemoved(context, namespace);

        NamespaceRemovedEvent event = new NamespaceRemovedEvent(namespaceId, namespace);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> memberSubpath = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    members().
                    member(context.getMember().getId()).toList();
            DirectorySubspace memberSubspace = DirectoryLayer.getDefault().open(tr, memberSubpath).join();

            NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, context.getClusterName(), event);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> memberSubpath = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    members().
                    member(context.getMember().getId()).toList();
            DirectorySubspace memberSubspace = DirectoryLayer.getDefault().open(tr, memberSubpath).join();

            Long version = NamespaceUtil.readLastSeenNamespaceVersion(tr, memberSubspace, namespaceId);
            assertNotNull(version);
            assertThat(version).isEqualTo(1);
        }
    }
}