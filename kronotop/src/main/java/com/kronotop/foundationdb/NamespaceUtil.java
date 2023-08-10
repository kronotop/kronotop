/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.foundationdb;

import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;

import java.util.ArrayList;
import java.util.List;

class NamespaceUtil {
    static String NoSuchNamespaceMessage(Context context, Throwable throwable) {
        if (!(throwable instanceof NoSuchDirectoryException)) {
            throw new IllegalArgumentException("");
        }

        NoSuchDirectoryException exception = (NoSuchDirectoryException) throwable;
        List<String> basePath = DirectoryLayout.Builder.clusterName(context.getClusterName()).namespaces().asList();
        List<String> differences = new ArrayList<>(exception.path);
        differences.removeAll(basePath);
        return String.format("No such namespace: %s", String.join(".", differences));
    }

    static String NamespaceAlreadyExistsMessage(Context context, Throwable throwable) {
        if (!(throwable instanceof DirectoryAlreadyExistsException)) {
            throw new IllegalArgumentException("");
        }

        DirectoryAlreadyExistsException exception = (DirectoryAlreadyExistsException) throwable;
        List<String> basePath = DirectoryLayout.Builder.clusterName(context.getClusterName()).namespaces().asList();
        List<String> differences = new ArrayList<>(exception.path);
        differences.removeAll(basePath);
        return String.format("Namespace already exists: %s", String.join(".", differences));
    }
}