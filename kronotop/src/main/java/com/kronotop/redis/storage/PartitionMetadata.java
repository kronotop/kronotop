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

package com.kronotop.redis.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class PartitionMetadata {
    private long processId;
    private String address;
    private PartitionStatus status;
    private boolean isWritable;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @JsonIgnore
    public boolean isWritable() {
        return isWritable;
    }

    public PartitionStatus getStatus() {
        return status;
    }

    public void setStatus(PartitionStatus status) {
        this.status = status;
        if (status.equals(PartitionStatus.WRITABLE)) {
            isWritable = true;
        }
    }

    public long getProcessId() {
        return processId;
    }

    public void setProcessId(long processId) {
        this.processId = processId;
    }

    @Override
    public String toString() {
        return String.format(
                "PartitionMetadata {address=%s processId=%s status=%s}",
                address,
                processId,
                status
        );
    }
}
