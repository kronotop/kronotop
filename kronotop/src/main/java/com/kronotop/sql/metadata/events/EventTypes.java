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

package com.kronotop.sql.metadata.events;

/**
 * The EventTypes enum represents different types of events.
 * <p>
 * This enum provides constants for the different types of events that can occur in the system.
 * Each constant represents a specific type of event.
 * <p>
 * It is used as a parameter in the constructor of {@link BaseMetadataEvent} class to specify the type of event.
 */
public enum EventTypes {
    SCHEMA_CREATED,
    TABLE_CREATED,
    SCHEMA_DROPPED,
    TABLE_DROPPED,
    TABLE_RENAMED,
    TABLE_ALTERED
}
