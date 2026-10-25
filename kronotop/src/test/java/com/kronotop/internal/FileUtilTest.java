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

package com.kronotop.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileUtilTest {

    @Test
    void shouldDeleteEmptyDirectory(@TempDir Path base) throws IOException {
        // Behavior: an existing empty directory is removed.
        Path directory = Files.createDirectory(base.resolve("empty"));
        assertTrue(Files.isDirectory(directory));
        assertTrue(Files.exists(directory));

        FileUtil.deleteDirectory(directory);

        assertFalse(Files.exists(directory));
    }

    @Test
    void shouldDeleteDirectoryWithFiles(@TempDir Path base) throws IOException {
        // Behavior: every file inside the directory is removed together with the directory.
        Path directory = Files.createDirectory(base.resolve("with-files"));
        Path file1 = Files.createFile(directory.resolve("a.txt"));
        Path file2 = Files.createFile(directory.resolve("b.txt"));
        assertTrue(Files.exists(file1));
        assertTrue(Files.exists(file2));

        FileUtil.deleteDirectory(directory);

        assertFalse(Files.exists(directory));
        assertFalse(Files.exists(file1));
        assertFalse(Files.exists(file2));
    }

    @Test
    void shouldDeleteNestedDirectoryTree(@TempDir Path base) throws IOException {
        // Behavior: the whole tree, including nested subdirectories, is removed.
        Path root = Files.createDirectory(base.resolve("tree"));
        Path level1 = Files.createDirectory(root.resolve("level1"));
        Path level2 = Files.createDirectory(level1.resolve("level2"));
        Path leaf = Files.createFile(level2.resolve("deep.txt"));
        assertTrue(Files.exists(leaf));

        FileUtil.deleteDirectory(root);

        assertFalse(Files.exists(root));
        assertFalse(Files.exists(level1));
        assertFalse(Files.exists(level2));
        assertFalse(Files.exists(leaf));
        // The sibling root of the temp base is not part of the removed subtree.
        assertTrue(Files.exists(base));
    }

    @Test
    void shouldLeaveSiblingUntouched(@TempDir Path base) throws IOException {
        // Behavior: deleting one directory does not affect a sibling directory.
        Path target = Files.createDirectory(base.resolve("target"));
        Path sibling = Files.createDirectory(base.resolve("sibling"));
        Path kept = Files.createFile(sibling.resolve("keep.txt"));
        assertTrue(Files.exists(target));
        assertTrue(Files.exists(kept));

        FileUtil.deleteDirectory(target);

        assertFalse(Files.exists(target));
        assertTrue(Files.exists(sibling));
        assertTrue(Files.exists(kept));
    }

    @Test
    void shouldThrowWhenDirectoryDoesNotExist(@TempDir Path base) {
        // Behavior: a missing directory yields a NoSuchFileException, a subtype of IOException.
        Path missing = base.resolve("does-not-exist");
        assertFalse(Files.exists(missing));

        assertThrows(NoSuchFileException.class, () -> FileUtil.deleteDirectory(missing));
    }

    @Test
    void shouldThrowWhenArgumentIsNull() {
        // Behavior: null is not a valid directory and is rejected.
        assertThrows(NullPointerException.class, () -> FileUtil.deleteDirectory(null));
    }
}
