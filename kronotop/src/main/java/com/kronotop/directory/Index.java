// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.directory;

import java.util.List;

public class Index extends KronotopDirectoryNode {
    Index(List<String> layout, String bucket, String index) {
        super(layout);
        layout.add("indexes");
        layout.add(bucket);
        layout.add(index);
    }
}
