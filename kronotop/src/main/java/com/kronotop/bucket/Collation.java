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

package com.kronotop.bucket;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public record Collation(
        @JsonProperty("locale") String locale,
        @JsonProperty("strength") int strength,
        @JsonProperty("case_level") boolean caseLevel,
        @JsonProperty("case_first") String caseFirst,
        @JsonProperty("numeric_ordering") boolean numericOrdering,
        @JsonProperty("alternate") String alternate,
        @JsonProperty("backwards") boolean backwards,
        @JsonProperty("normalization") boolean normalization,
        @JsonProperty("max_variable") String maxVariable,
        @JsonIgnore int hash
) {
    private static final Set<String> VALID_CASE_FIRST = Set.of("upper", "lower", "off");
    private static final Set<String> VALID_ALTERNATE = Set.of("non-ignorable", "shifted");
    private static final Set<String> VALID_MAX_VARIABLE = Set.of("punct", "space");

    @JsonCreator
    public static Collation create(
            @JsonProperty("locale") String locale,
            @JsonProperty("strength") Integer strength,
            @JsonProperty("case_level") Boolean caseLevel,
            @JsonProperty("case_first") String caseFirst,
            @JsonProperty("numeric_ordering") Boolean numericOrdering,
            @JsonProperty("alternate") String alternate,
            @JsonProperty("backwards") Boolean backwards,
            @JsonProperty("normalization") Boolean normalization,
            @JsonProperty("max_variable") String maxVariable
    ) {
        strength = Objects.requireNonNullElse(strength, 3);
        caseLevel = Objects.requireNonNullElse(caseLevel, false);
        caseFirst = Objects.requireNonNullElse(caseFirst, "off");
        numericOrdering = Objects.requireNonNullElse(numericOrdering, false);
        alternate = Objects.requireNonNullElse(alternate, "non-ignorable");
        backwards = Objects.requireNonNullElse(backwards, false);
        normalization = Objects.requireNonNullElse(normalization, false);
        maxVariable = Objects.requireNonNullElse(maxVariable, "punct");

        int hash = Objects.hash(locale, strength, caseLevel, caseFirst,
                numericOrdering, alternate, backwards, normalization, maxVariable);

        return new Collation(
                locale,
                strength,
                caseLevel,
                caseFirst,
                numericOrdering,
                alternate,
                backwards,
                normalization,
                maxVariable,
                hash
        );
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public void validate() {
        if (locale == null || locale.isBlank()) {
            throw new IllegalArgumentException("Collation 'locale' is required");
        }

        ULocale uLocale = new ULocale(locale);
        boolean supported = Arrays.asList(Collator.getAvailableULocales()).contains(uLocale);
        if (!supported) {
            throw new IllegalArgumentException("Invalid collation locale: " + locale);
        }

        if (strength < 1 || strength > 5) {
            throw new IllegalArgumentException("Collation 'strength' must be between 1 and 5, got: " + strength);
        }

        if (!VALID_CASE_FIRST.contains(caseFirst)) {
            throw new IllegalArgumentException("Collation 'case_first' must be one of 'upper', 'lower', 'off', got: " + caseFirst);
        }

        if (!VALID_ALTERNATE.contains(alternate)) {
            throw new IllegalArgumentException("Collation 'alternate' must be one of 'non-ignorable', 'shifted', got: " + alternate);
        }

        if (!VALID_MAX_VARIABLE.contains(maxVariable)) {
            throw new IllegalArgumentException("Collation 'max_variable' must be one of 'punct', 'space', got: " + maxVariable);
        }
    }
}
