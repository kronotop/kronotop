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

package com.kronotop.commands;

import com.kronotop.server.CommandType;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

/**
 * Loads command definitions from JSON files on the classpath.
 * <p>
 * Every file maps a command name to its metadata. A definition with a container field is a
 * subcommand and is attached to its parent. Top-level names must exist in {@link CommandType}.
 */
public final class CommandMetadataLoader {
    public static final String DEFAULT_RESOURCE_DIR = "commands";
    private static final String SUFFIX = ".json";
    private static final JsonMapper MAPPER = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .build();
    private static final TypeReference<LinkedHashMap<String, CommandMetadata>> FILE_TYPE = new TypeReference<>() {
    };

    private CommandMetadataLoader() {
    }

    /**
     * Loads all definitions under the default resource directory.
     */
    public static Map<String, CommandMetadata> load() {
        return load(DEFAULT_RESOURCE_DIR);
    }

    /**
     * Loads all definitions under the given classpath directory.
     *
     * @throws IllegalStateException if a definition is invalid or cannot be linked
     */
    public static Map<String, CommandMetadata> load(String resourceDir) {
        ClassLoader classLoader = CommandMetadataLoader.class.getClassLoader();
        Map<String, CommandMetadata> commands = new LinkedHashMap<>();
        Map<String, Map<String, CommandMetadata>> pending = new LinkedHashMap<>();

        for (String resource : listResources(classLoader, resourceDir)) {
            for (Map.Entry<String, CommandMetadata> entry : parse(classLoader, resource).entrySet()) {
                String name = entry.getKey().toUpperCase();
                CommandMetadata metadata = entry.getValue();
                if (metadata.container() == null) {
                    if (CommandType.parse(name) == null) {
                        throw new IllegalStateException(resource + ": unknown command '" + name + "'");
                    }
                    if (commands.putIfAbsent(name, metadata) != null) {
                        throw new IllegalStateException(resource + ": duplicate command '" + name + "'");
                    }
                } else {
                    String container = metadata.container().toUpperCase();
                    Map<String, CommandMetadata> subcommands = pending.computeIfAbsent(container, ignored -> new LinkedHashMap<>());
                    if (subcommands.putIfAbsent(name, metadata) != null) {
                        throw new IllegalStateException(resource + ": duplicate subcommand '" + container + " " + name + "'");
                    }
                }
            }
        }

        for (Map.Entry<String, Map<String, CommandMetadata>> entry : pending.entrySet()) {
            CommandMetadata parent = commands.get(entry.getKey());
            if (parent == null) {
                throw new IllegalStateException("unknown container '" + entry.getKey() + "' for subcommands " + entry.getValue().keySet());
            }
            commands.put(entry.getKey(), parent.withSubcommands(entry.getValue()));
        }
        return Collections.unmodifiableMap(commands);
    }

    private static Map<String, CommandMetadata> parse(ClassLoader classLoader, String resource) {
        try (InputStream inputStream = classLoader.getResourceAsStream(resource)) {
            if (inputStream == null) {
                throw new IllegalStateException(resource + ": resource not found");
            }
            return MAPPER.readValue(inputStream, FILE_TYPE);
        } catch (IOException e) {
            throw new UncheckedIOException(resource, e);
        } catch (RuntimeException e) {
            throw new IllegalStateException(resource + ": " + e.getMessage(), e);
        }
    }

    private static List<String> listResources(ClassLoader classLoader, String dir) {
        Set<String> resources = new TreeSet<>();
        try {
            Enumeration<URL> urls = classLoader.getResources(dir);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                switch (url.getProtocol()) {
                    case "file" -> listFromDirectory(url, dir, resources);
                    case "jar" -> listFromJar(url, dir, resources);
                    default -> throw new IllegalStateException("unsupported resource protocol: " + url);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
        return new ArrayList<>(resources);
    }

    private static void listFromDirectory(URL url, String dir, Set<String> resources) throws IOException, URISyntaxException {
        try (Stream<Path> files = Files.list(Path.of(url.toURI()))) {
            files.map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(SUFFIX))
                    .forEach(name -> resources.add(dir + "/" + name));
        }
    }

    private static void listFromJar(URL url, String dir, Set<String> resources) throws IOException {
        JarURLConnection connection = (JarURLConnection) url.openConnection();
        connection.setUseCaches(false);
        String prefix = dir + "/";
        try (JarFile jar = connection.getJarFile()) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                String name = entries.nextElement().getName();
                if (name.startsWith(prefix) && name.endsWith(SUFFIX) && name.indexOf('/', prefix.length()) < 0) {
                    resources.add(name);
                }
            }
        }
    }
}
