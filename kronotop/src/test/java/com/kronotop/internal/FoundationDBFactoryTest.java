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

package com.kronotop.internal;

import com.kronotop.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FoundationDBFactoryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRejectMissingClientLibrary() {
        // Behavior: checkClientLibrary with a path that does not exist throws a ConfigException with the message
        // "No such FoundationDB client library: '<path>'".
        String path = tempDir.resolve("libfdb_c.dylib").toString();

        ConfigException exception = assertThrows(ConfigException.class,
                () -> FoundationDBFactory.checkClientLibrary(path));

        assertEquals("No such FoundationDB client library: '" + path + "'", exception.getMessage());
    }

    @Test
    void shouldAcceptExistingClientLibrary() throws IOException {
        // Behavior: checkClientLibrary with the path of an existing file returns without throwing.
        Path library = Files.createFile(tempDir.resolve("libfdb_c.dylib"));

        assertDoesNotThrow(() -> FoundationDBFactory.checkClientLibrary(library.toString()));
    }
}
