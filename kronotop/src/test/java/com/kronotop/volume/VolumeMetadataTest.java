/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.kronotop.BaseTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class VolumeMetadataTest extends BaseTest {
    @Test
    public void test_addSegment() {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        volumeMetadata.addSegment(1);

        assertThrows(IllegalArgumentException.class, () -> volumeMetadata.addSegment(1));
    }

    @Test
    public void test_removeSegment() {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        volumeMetadata.addSegment(1);
        volumeMetadata.removeSegment(1);

        assertThrows(IllegalArgumentException.class, () -> volumeMetadata.removeSegment(1));
    }
}