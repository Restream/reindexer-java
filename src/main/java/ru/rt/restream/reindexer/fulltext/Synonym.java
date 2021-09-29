/*
 * Copyright 2020 Restream
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.rt.restream.reindexer.fulltext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * List of synonyms to use for search of documents.
 * Part of options of {@link FullTextConfig}.
 */
public class Synonym {

    /**
     * List source tokens in a query, which will be replaced with alternatives.
     */
    private List<String> tokens;

    /**
     * List of alternatives, which will be used for search documents.
     */
    private List<String> alternatives;

    public Synonym() {
        this(new ArrayList<>(), new ArrayList<>());
    }

    public Synonym(List<String> tokens, List<String> alternatives) {
        this.tokens = tokens;
        this.alternatives = alternatives;
    }

    public Synonym(String[] tokens, String[] alternatives) {
        this.tokens = Arrays.asList(tokens);
        this.alternatives = Arrays.asList(alternatives);
    }

    public List<String> getTokens() {
        return tokens;
    }

    public void setTokens(List<String> tokens) {
        this.tokens = tokens;
    }

    public List<String> getAlternatives() {
        return alternatives;
    }

    public void setAlternatives(List<String> alternatives) {
        this.alternatives = alternatives;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Synonym synonym = (Synonym) o;
        return Objects.equals(tokens, synonym.tokens)
                && Objects.equals(alternatives, synonym.alternatives);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokens, alternatives);
    }
}
