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

package com.kronotop.cli;

import com.kronotop.cli.CommandDocsCatalog.CommandDoc;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/**
 * Renders a help entry for the interactive "help" command.
 * The output starts with a blank line and ends with a newline. Empty fields are skipped.
 */
public final class HelpFormatter {

    private static final AttributedStyle NAME_STYLE = AttributedStyle.DEFAULT.bold();
    private static final AttributedStyle USAGE_STYLE = AttributedStyle.DEFAULT.faint();
    private static final AttributedStyle KEY_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW);

    private HelpFormatter() {
    }

    /**
     * Formats the entry. A null terminal produces plain text without escape codes.
     */
    public static String format(CommandDoc doc, Terminal terminal) {
        AttributedStringBuilder builder = new AttributedStringBuilder();
        builder.append("\n  ").append(doc.name(), NAME_STYLE);
        String usage = doc.usageLine();
        if (!usage.isEmpty()) {
            builder.append(" ").append(usage, USAGE_STYLE);
        }
        builder.append("\n");
        field(builder, "summary", doc.summary());
        field(builder, "since", doc.since());
        field(builder, "group", doc.group());
        return terminal == null ? builder.toString() : builder.toAnsi(terminal);
    }

    private static void field(AttributedStringBuilder builder, String key, String value) {
        if (value.isEmpty()) {
            return;
        }
        builder.append("  ").append(key + ":", KEY_STYLE).append(" ").append(value).append("\n");
    }
}
