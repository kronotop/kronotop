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

package com.kronotop.ctl;

import java.util.ArrayList;
import java.util.List;

/**
 * A text table with left-aligned columns separated by two spaces.
 */
public final class TextTable {
    private static final int COLUMN_GAP = 2;

    private final List<String[]> rows = new ArrayList<>();

    public TextTable(String... headers) {
        rows.add(headers);
    }

    public void addRow(String... cells) {
        rows.add(cells);
    }

    /**
     * Returns the table as text with the header first and one line per row.
     *
     * @return the text, ending with a newline
     */
    public String render() {
        int columns = rows.getFirst().length;
        int[] widths = new int[columns];
        for (String[] row : rows) {
            for (int i = 0; i < columns; i++) {
                widths[i] = Math.max(widths[i], row[i].length());
            }
        }

        StringBuilder sb = new StringBuilder();
        for (String[] row : rows) {
            for (int i = 0; i < columns; i++) {
                sb.append(row[i]);
                // The last column is not padded, so lines have no trailing spaces.
                if (i < columns - 1) {
                    sb.repeat(" ", widths[i] - row[i].length() + COLUMN_GAP);
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }
}
