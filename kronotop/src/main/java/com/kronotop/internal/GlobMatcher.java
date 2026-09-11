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
package com.kronotop.internal;

/**
 * Glob style pattern matching with the same rules as Redis: '*', '?', '[abc]', '[a-z]', '[^a]' and '\\' escapes.
 */
public final class GlobMatcher {
    private GlobMatcher() {
    }

    /**
     * Returns true when the string matches the pattern.
     */
    public static boolean matches(String pattern, String string, boolean noCase) {
        return match(pattern, 0, string, 0, noCase);
    }

    private static boolean match(String pattern, int p, String string, int s, boolean noCase) {
        int patternLen = pattern.length();
        int stringLen = string.length();
        while (p < patternLen && s < stringLen) {
            char pc = pattern.charAt(p);
            switch (pc) {
                case '*' -> {
                    while (p + 1 < patternLen && pattern.charAt(p + 1) == '*') {
                        p++;
                    }
                    if (p + 1 == patternLen) {
                        return true;
                    }
                    for (int i = s; i < stringLen; i++) {
                        if (match(pattern, p + 1, string, i, noCase)) {
                            return true;
                        }
                    }
                    return false;
                }
                case '?' -> s++;
                case '[' -> {
                    p++;
                    boolean not = p < patternLen && pattern.charAt(p) == '^';
                    if (not) {
                        p++;
                    }
                    boolean matched = false;
                    char sc = string.charAt(s);
                    while (true) {
                        if (p < patternLen && pattern.charAt(p) == '\\' && p + 1 < patternLen) {
                            p++;
                            if (pattern.charAt(p) == sc) {
                                matched = true;
                            }
                        } else if (p < patternLen && pattern.charAt(p) == ']') {
                            break;
                        } else if (p == patternLen) {
                            p--;
                            break;
                        } else if (p + 2 < patternLen && pattern.charAt(p + 1) == '-') {
                            char start = pattern.charAt(p);
                            char end = pattern.charAt(p + 2);
                            char c = sc;
                            if (start > end) {
                                char tmp = start;
                                start = end;
                                end = tmp;
                            }
                            if (noCase) {
                                start = Character.toLowerCase(start);
                                end = Character.toLowerCase(end);
                                c = Character.toLowerCase(c);
                            }
                            p += 2;
                            if (c >= start && c <= end) {
                                matched = true;
                            }
                        } else {
                            if (same(pattern.charAt(p), sc, noCase)) {
                                matched = true;
                            }
                        }
                        p++;
                    }
                    if (not) {
                        matched = !matched;
                    }
                    if (!matched) {
                        return false;
                    }
                    s++;
                }
                case '\\' -> {
                    if (p + 1 < patternLen) {
                        p++;
                        pc = pattern.charAt(p);
                    }
                    if (!same(pc, string.charAt(s), noCase)) {
                        return false;
                    }
                    s++;
                }
                default -> {
                    if (!same(pc, string.charAt(s), noCase)) {
                        return false;
                    }
                    s++;
                }
            }
            p++;
            if (s == stringLen) {
                while (p < patternLen && pattern.charAt(p) == '*') {
                    p++;
                }
                break;
            }
        }
        return p == patternLen && s == stringLen;
    }

    private static boolean same(char a, char b, boolean noCase) {
        return noCase ? Character.toLowerCase(a) == Character.toLowerCase(b) : a == b;
    }
}
