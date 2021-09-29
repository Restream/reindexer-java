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
package ru.rt.restream.reindexer.annotations;

import ru.rt.restream.reindexer.fulltext.FullTextConfig;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use the @FullText annotation in code to tune the full text search params of text index.
 * Use it only in conjunction with {@link Reindex} annotation of text type.
 * About default values and usage see
 * <a href="https://github.com/Restream/reindexer/blob/master/fulltext.md#limitations-and-know-issues">
 * Limitations and know issues</a>
 * Does not check values for out of bounds, if you used too small or too big value,
 * you have exception when you add or update the index.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FullText {

    /**
     * Enable wrong keyboard layout variants processing. e.g. term "keynbr" will match word "лунтик".
     *
     * @return true, if wrong keyboard layout processing is enabled
     */
    boolean enableKbLayout() default true;

    /**
     * Enable numbers search.
     *
     * @return true, if number search is enabled
     */
    boolean enableNumbersSearch() default false;

    /**
     * Enable russian translit variants processing. E.g. term "luntik" will match word "лунтик".
     *
     * @return true, if russian translit processing is enabled
     */
    boolean enableTranslit() default true;

    /**
     * Enable automatic index warmup after transaction, which has performed namespace copy.
     *
     * @return true, if automatic index warmup after namespace copy is enabled
     */
    boolean enableWarmupOnNsCopy() default false;

    /**
     * Boost of bm25 ranking.
     *
     * @return boost of bm25 ranking
     */
    double bm25Boost() default 1.0;

    /**
     * Weight of bm25 rank in final rank.
     *
     * @return weight of bm25 rank in final rank
     */
    double bm25Weight() default 0.5;

    /**
     * Boost of search query term position.
     *
     * @return boost of search query term position
     */
    double positionBoost() default 1.0;

    /**
     * Weight of search query term position in final rank.
     *
     * @return weight of search query term position in final rank
     */
    double positionWeight() default 0.1;

    /**
     * Boost of search query term distance in found document.
     *
     * @return boost of search query term distance in found document
     */
    double distanceBoost() default 1.0;

    /**
     * Weight of search query terms distance in found document in final rank.
     *
     * @return weight of search query terms distance in found document in final rank
     */
    double distanceWeight() default 0.5;

    /**
     * Boost of search query term length.
     *
     * @return boost of search query term length
     */
    double termLenBoost() default 1.0;

    /**
     * Weight of search query term length in final rank.
     *
     * @return weight of search query term length in final rank
     */
    double termLenWeight() default 0.3;

    /**
     * Boost of full match of search phrase with document.
     *
     * @return boost of full match of search phrase with document
     */
    double fullMatchBoost() default 1.0;

    /**
     * Minimal rank of found documents, only documents with relevancy greater or equal minRelevancy will be returned.
     *
     * @return minimal rank of found documents
     */
    double minRelevancy() default 0.05;

    /**
     * Ratio of summation of ranks of match one term in several fields.
     *
     * @return ratio of summation of ranks of match one term in several fields
     */
    double sumRanksByFieldsRatio() default 0.0;

    /**
     * Log level of full text search engine, the range is from 0 to 4.
     *
     * @return log level of full text search engine
     */
    int logLevel() default 0;

    /**
     * Maximum documents count which will be processed in merge query results.
     * Increasing this value might refine ranking of queries with high frequency words, but will decrease search speed.
     *
     * @return maximum documents count which will be processed in merge query results
     */
    int mergeLimit() default 20000;

    /**
     * Maximum possible typos in word.
     * 0: typos is disabled, words with typos will not match. N: words with N possible typos will match.
     * It is not recommended to set more than 2 possible typo
     * - It'll greatly increase the RAM usage, and decrease the search speed.
     *
     * @return maximum number of possible typos in word
     */
    int maxTypos() default 2;

    /**
     * Maximum word length for building and matching variants with typos.
     *
     * @return maximum word length for building and matching variants with typos
     */
    int maxTypoLen() default 15;

    /**
     * Maximum steps without full rebuild of ft - more steps faster commit slower select - optimal about 15.
     *
     * @return maximum steps count without full rebuild of full text index
     */
    int maxRebuildSteps() default 50;

    /**
     * Maximum number of unique words to step.
     *
     * @return maximum number of unique words to step
     */
    int maxStepSize() default 4000;

    /**
     * Decrease of relevancy in case of partial match by value:
     * {@code partial_match_decrease * (non matched symbols) / (matched symbols)}
     *
     * @return decrease of relevancy in case of partial match
     */
    int partialMatchDecrease() default 15;

    /**
     * List of stemmers to use. Default value - {"en", "ru"}.
     * Stemmer - algorithm that reduce words to stem - immutable part of word
     *
     * @return list of stemmers
     */
    String[] stemmers() default {"en", "ru"};

    /**
     * List of stop words. Words from this list will be ignored in documents and queries.
     *
     * @return list of stop words
     */
    String[] stopWords() default {};

    /**
     * List of synonyms. If query has a word-token, words-alternatives will be used for search.
     *
     * @return list of synonyms
     */
    Synonym[] synonyms() default {};

    /**
     * Extra symbols, which will be treated as a part of word to addition to letters and digits.
     *
     * @return additional symbols, which will be treated as a part of word
     */
    String extraWordSymbols() default "-/+";

    /**
     * List of synonyms for replacement.
     * Part of options of {@link FullTextConfig}.
     */
    @interface Synonym {

        /**
         * List source tokens in a query, which will be replaced with alternatives.
         *
         * @return list source tokens, which will be replaced with alternatives
         */
        String[] tokens() default {};

        /**
         * List of alternatives, which will be used for search documents.
         *
         * @return list of alternatives, which will be used for search documents
         */
        String[] alternatives() default {};

    }
}
