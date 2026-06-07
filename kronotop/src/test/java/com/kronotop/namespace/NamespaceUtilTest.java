/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.BaseHandlerTest;
import com.kronotop.DataStructureKind;
import com.kronotop.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.namespace.handlers.Namespace;
import com.kronotop.namespace.handlers.NamespaceMetadata;
import com.kronotop.namespace.handlers.NamespaceRemovedEvent;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

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
            DirectorySubspace subspace = NamespaceUtil.open(tr, context, namespace, DataStructureKind.BUCKET);
            assertThat(subspace).isNotNull();
        }
    }

    @Test
    void shouldThrowExceptionWhenOpeningNonExistentNamespace() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThatThrownBy(() -> NamespaceUtil.open(tr, context, "non.existent.namespace", DataStructureKind.BUCKET))
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
            NamespaceUtil.remove(tr, context, List.of("test", "namespace", "remove", "transaction"));
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
            assertThatThrownBy(() -> NamespaceUtil.checkBeingRemoved(tr, context, List.of("hierarchy", "parent", "child")))
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
            assertThatThrownBy(() -> NamespaceUtil.checkBeingRemoved(tr, context, List.of("a", "b", "c", "d")))
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
            assertThatThrownBy(() -> NamespaceUtil.checkBeingRemoved(tr, context, List.of("deep", "level1", "level2", "level3")))
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
            NamespaceUtil.checkBeingRemoved(tr, context, List.of("clean", "hierarchy", "child"));
        }
    }

    @Test
    void shouldSetAndReadLastSeenNamespaceVersion() {
        String namespace = "test.namespace.version.tracking";
        NamespaceUtil.create(context, namespace);

        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
        UUID namespaceId = metadata.id();

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
            DirectorySubspace memberSubspace = context.getDirectoryLayer().open(tr, memberSubpath).join();

            NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, context, event);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> memberSubpath = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    members().
                    member(context.getMember().getId()).toList();
            DirectorySubspace memberSubspace = context.getDirectoryLayer().open(tr, memberSubpath).join();

            Long version = NamespaceUtil.readLastSeenNamespaceVersion(tr, memberSubspace, namespaceId);
            assertNotNull(version);
            assertThat(version).isEqualTo(1);
        }
    }

    @Test
    void shouldCreateMetadataForIntermediateNamespaceLevels() {
        // Behavior: Creating a deep namespace writes metadata for all intermediate levels
        NamespaceUtil.create(context, "im.level.test.leaf");

        NamespaceMetadata rootMeta = NamespaceUtil.readMetadata(context, "im");
        assertThat(rootMeta.id()).isNotNull();
        assertThat(rootMeta.name()).isEqualTo("im");
        assertThat(rootMeta.removed()).isFalse();
        assertThat(rootMeta.version()).isEqualTo(0);

        NamespaceMetadata midMeta = NamespaceUtil.readMetadata(context, "im.level");
        assertThat(midMeta.id()).isNotNull();
        assertThat(midMeta.name()).isEqualTo("im.level");

        NamespaceMetadata deepMeta = NamespaceUtil.readMetadata(context, "im.level.test");
        assertThat(deepMeta.id()).isNotNull();
        assertThat(deepMeta.name()).isEqualTo("im.level.test");

        NamespaceMetadata leafMeta = NamespaceUtil.readMetadata(context, "im.level.test.leaf");
        assertThat(leafMeta.id()).isNotNull();
        assertThat(leafMeta.name()).isEqualTo("im.level.test.leaf");

        assertThat(rootMeta.id()).isNotEqualTo(midMeta.id());
        assertThat(midMeta.id()).isNotEqualTo(deepMeta.id());
        assertThat(deepMeta.id()).isNotEqualTo(leafMeta.id());
    }

    @Test
    void shouldNotOverwriteExistingIntermediateMetadata() {
        // Behavior: Creating a child namespace does not overwrite existing parent metadata
        NamespaceUtil.create(context, "pre.existing");
        NamespaceMetadata originalMeta = NamespaceUtil.readMetadata(context, "pre.existing");
        UUID originalId = originalMeta.id();

        NamespaceUtil.create(context, "pre.existing.child.leaf");
        NamespaceMetadata afterMeta = NamespaceUtil.readMetadata(context, "pre.existing");
        assertThat(afterMeta.id()).isEqualTo(originalId);
        assertThat(afterMeta.name()).isEqualTo("pre.existing");
    }

    @Test
    void shouldAllowRemoveOnIntermediateNamespace() {
        // Behavior: Removing an intermediate namespace works because it has proper metadata
        NamespaceUtil.create(context, "rem.inter.deep.leaf");

        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, "rem.inter");
        assertThat(metadata.id()).isNotNull();

        NamespaceUtil.setRemoved(context, "rem.inter");

        NamespaceMetadata afterRemoval = NamespaceUtil.readMetadata(context, "rem.inter");
        assertThat(afterRemoval.removed()).isTrue();
        assertThat(afterRemoval.version()).isEqualTo(1);
    }

    @Test
    void shouldCreateSingleLevelNamespaceWithoutIntermediateProcessing() {
        // Behavior: A single-level namespace has no intermediates to process
        NamespaceUtil.create(context, "singlelevel");

        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, "singlelevel");
        assertThat(metadata.id()).isNotNull();
        assertThat(metadata.name()).isEqualTo("singlelevel");
    }

    @Test
    void shouldThrowExceptionWhenNamespaceDepthExceedsMaximum() {
        // Behavior: Namespace depth is limited to 10 levels
        String namespace = IntStream.rangeClosed(1, 11).mapToObj(i -> "level" + i).collect(Collectors.joining("."));

        assertThatThrownBy(() -> NamespaceUtil.create(context, namespace))
                .isInstanceOf(KronotopException.class)
                .hasMessageContaining("Namespace depth exceeds maximum allowed depth of 10");
    }

    @Test
    void shouldResolveNamespacePath() {
        // Behavior: Resolves the full dotted namespace path from a subspace by walking parent pointers
        String namespace = "im.level.test.leaf";
        NamespaceUtil.create(context, namespace);
        DirectorySubspace subspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            subspace = NamespaceUtil.open(tr, context, namespace);
        }
        List<String> subpath = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NamespaceUtil.resolveNamespacePath(tr, new Subspace(subspace.pack()), subpath);
        }

        assertEquals(namespace, String.join(".", subpath.reversed()));
    }

    private List<String> fdbPath(List<String> subpath) {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                namespaces().
                namespace(subpath).
                toList();
    }

    private String resolveAfterMove(DirectorySubspace movedSubspace) {
        List<String> subpath = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NamespaceUtil.resolveNamespacePath(tr, new Subspace(movedSubspace.pack()), subpath);
        }
        return String.join(".", subpath.reversed());
    }

    @Test
    void shouldResolveNamespacePathAfterMoveWithinSameParent() {
        // Behavior: After moving a namespace within the same parent, resolveNamespacePath returns the new path
        NamespaceUtil.create(context, "mv.same.old");

        DirectorySubspace movedSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            context.getDirectoryLayer().move(tr, fdbPath(List.of("mv", "same", "old")), fdbPath(List.of("mv", "same", "renamed"))).join();
            movedSubspace = context.getDirectoryLayer().open(tr, fdbPath(List.of("mv", "same", "renamed"))).join();
            NamespaceUtil.updateLeafAndParentPointer(context, tr, movedSubspace, List.of("mv", "same", "renamed"));
            tr.commit().join();
        }

        assertEquals("mv.same.renamed", resolveAfterMove(movedSubspace));
    }

    @Test
    void shouldResolveNamespacePathAfterMoveToDifferentParent() {
        // Behavior: After moving a namespace to a different parent, resolveNamespacePath returns the new path
        NamespaceUtil.create(context, "mv.src.child");
        NamespaceUtil.create(context, "mv.dst");

        DirectorySubspace movedSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            context.getDirectoryLayer().move(tr, fdbPath(List.of("mv", "src", "child")), fdbPath(List.of("mv", "dst", "child"))).join();
            movedSubspace = context.getDirectoryLayer().open(tr, fdbPath(List.of("mv", "dst", "child"))).join();
            NamespaceUtil.updateLeafAndParentPointer(context, tr, movedSubspace, List.of("mv", "dst", "child"));
            tr.commit().join();
        }

        assertEquals("mv.dst.child", resolveAfterMove(movedSubspace));
    }

    @Test
    void shouldResolveNamespacePathAfterMoveToRoot() {
        // Behavior: After moving a nested namespace to root, resolveNamespacePath returns the root name (PARENT_POINTER cleared)
        NamespaceUtil.create(context, "mv.nested.deep");

        DirectorySubspace movedSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            context.getDirectoryLayer().move(tr, fdbPath(List.of("mv", "nested", "deep")), fdbPath(List.of("mvrootarget"))).join();
            movedSubspace = context.getDirectoryLayer().open(tr, fdbPath(List.of("mvrootarget"))).join();
            NamespaceUtil.updateLeafAndParentPointer(context, tr, movedSubspace, List.of("mvrootarget"));
            tr.commit().join();
        }

        assertEquals("mvrootarget", resolveAfterMove(movedSubspace));
    }

    @Test
    void shouldResolveNamespacePathAfterMoveFromRootToNested() {
        // Behavior: After moving a root namespace into a nested position, resolveNamespacePath returns the new nested path
        NamespaceUtil.create(context, "mvrootsrc");
        NamespaceUtil.create(context, "mv.target");

        DirectorySubspace movedSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            context.getDirectoryLayer().move(tr, fdbPath(List.of("mvrootsrc")), fdbPath(List.of("mv", "target", "nested"))).join();
            movedSubspace = context.getDirectoryLayer().open(tr, fdbPath(List.of("mv", "target", "nested"))).join();
            NamespaceUtil.updateLeafAndParentPointer(context, tr, movedSubspace, List.of("mv", "target", "nested"));
            tr.commit().join();
        }

        assertEquals("mv.target.nested", resolveAfterMove(movedSubspace));
    }
}