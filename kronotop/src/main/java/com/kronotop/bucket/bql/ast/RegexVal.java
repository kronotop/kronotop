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

package com.kronotop.bucket.bql.ast;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import com.kronotop.bucket.bql.BqlParseException;

import java.util.Objects;

/**
 * Operand for the $regex operator. Holds the pattern, the option string, and the
 * compiled RE2 pattern. Matching uses RE2 (linear time, no catastrophic backtracking),
 * so backreferences and lookaround are not supported.
 * <p>
 * The pattern is compiled once when this value is constructed. An invalid pattern or
 * an unsupported option fails immediately as a parse error.
 */
public final class RegexVal implements BqlValue {
    private final String pattern;
    private final String options;
    private final Pattern compiled;

    public RegexVal(String pattern, String options) {
        if (pattern == null) {
            throw new BqlParseException("$regex pattern must not be null");
        }
        this.pattern = pattern;
        this.options = options == null ? "" : options;
        this.compiled = compile(this.pattern, this.options);
    }

    private static Pattern compile(String pattern, String options) {
        int flags = 0;
        for (int i = 0; i < options.length(); i++) {
            char c = options.charAt(i);
            switch (c) {
                case 'i' -> flags |= Pattern.CASE_INSENSITIVE;
                case 'm' -> flags |= Pattern.MULTILINE;
                case 's' -> flags |= Pattern.DOTALL;
                case 'u' -> {
                    // Unicode option: accepted but redundant, RE2 already operates on UTF-8.
                }
                default -> throw new BqlParseException("Unsupported $regex option: " + c);
            }
        }
        try {
            return Pattern.compile(pattern, flags);
        } catch (PatternSyntaxException e) {
            throw new BqlParseException("Invalid $regex pattern: " + e.getMessage());
        }
    }

    public String pattern() {
        return pattern;
    }

    public String options() {
        return options;
    }

    public Pattern compiled() {
        return compiled;
    }

    @Override
    public String toJson() {
        return "{\"$regex\":\"" + pattern + "\",\"$options\":\"" + options + "\"}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RegexVal other)) return false;
        return pattern.equals(other.pattern) && options.equals(other.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, options);
    }

    @Override
    public String toString() {
        return "RegexVal(pattern=" + pattern + ", options=" + options + ")";
    }
}
