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

package com.kronotop.volume;

/**
 * Immutable snapshot of a volume-level vacuum statistics and metadata: timing, outcome, and how many segments were processed.
 *
 * @param startedAt         epoch millis when the vacuum run began
 * @param completedAt       epoch millis when the vacuum run ended
 * @param result            outcome of the run
 * @param segmentsProcessed number of segments that were vacuumed
 */
public record VacuumMetadata(long startedAt, long completedAt, VacuumResult result, int segmentsProcessed) {
}
