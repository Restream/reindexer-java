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

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.rt.restream.reindexer.annotations.FullText;
import ru.rt.restream.reindexer.binding.definition.IndexConfig;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Contains the full text search  configuration for text index.
 * Has no public constructor, values of fields set with {@link FullText} annotation.
 * About default values and usage see
 * <a href="https://github.com/Restream/reindexer/blob/master/fulltext.md#limitations-and-know-issues">
 * Limitations and know issues</a>
 * Does not check values for out of bounds, if you used too small or too big value,
 * you have exception when you add or update the index.
 */
@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FullTextConfig implements IndexConfig {

    /**
     * Enable wrong keyboard layout variants processing. e.g. term "keynbr" will match word "лунтик".
     */
    private boolean enableKbLayout;

    /**
     * Enable numbers search.
     */
    private boolean enableNumbersSearch;

    /**
     * Enable russian translit variants processing. E.g. term "luntik" will match word "лунтик".
     */
    private boolean enableTranslit;

    /**
     * Enable automatic index warmup after transaction, which has performed namespace copy.
     */
    private boolean enableWarmupOnNsCopy;

    /**
     * Boost of bm25 ranking.
     */
    private double bm25Boost;

    /**
     * Weight of bm25 rank in final rank.
     */
    private double bm25Weight;

    /**
     * Boost of search query term position.
     */
    private double positionBoost;

    /**
     * Weight of search query term position in final rank.
     */
    private double positionWeight;

    /**
     * Boost of search query term distance in found document.
     */
    private double distanceBoost;

    /**
     * Weight of search query terms distance in found document in final rank.
     */
    private double distanceWeight;

    /**
     * Boost of search query term length.
     */
    private double termLenBoost;

    /**
     * Weight of search query term length in final rank.
     */
    private double termLenWeight;

    /**
     * Boost of full match of search phrase with document.
     */
    private double fullMatchBoost;

    /**
     * Minimal rank of found documents, only documents with relevancy >= minRelevancy will be returned.
     */
    private double minRelevancy;

    /**
     * Ratio of summation of ranks of match one term in several fields.
     */
    private double sumRanksByFieldsRatio;

    /**
     * Log level of full text search engine, the range is from 0 to 4.
     */
    private int logLevel;

    /**
     * Maximum documents count which will be processed in merge query results.
     * Increasing this value might refine ranking of queries with high frequency words, but will decrease search speed.
     */
    private int mergeLimit;

    /**
     * Maximum possible typos in word.
     * 0: typos is disabled, words with typos will not match. N: words with N possible typos will match.
     * It is not recommended to set more than 2 possible typo
     * - It'll greatly increase the RAM usage, and decrease the search speed.
     */
    private int maxTypos;

    /**
     * Maximum word length for building and matching variants with typos.
     */
    private int maxTypoLen;

    /**
     * Maximum steps without full rebuild of ft - more steps faster commit slower select - optimal about 15.
     */
    private int maxRebuildSteps;

    /**
     * Maximum unique words to step.
     */
    private int maxStepSize;

    /**
     * Decrease of relevancy in case of partial match by value:
     * {@code partial_match_decrease * (non matched symbols) / (matched symbols)}
     */
    private int partialMatchDecrease;

    /**
     * List of stemmers to use. Default value - {"en", "ru"}.
     */
    private List<String> stemmers;

    /**
     * List of stop words. Words from this list will be ignored in documents and queries.
     */
    private List<String> stopWords;

    /**
     * List of {@link Synonym}.
     */
    private List<Synonym> synonyms;

    /**
     * Extra symbols, which will be treated as a part of word to addition to letters and digits.
     */
    private String extraWordSymbols;

    public static FullTextConfig of(FullText annotation) {
        FullTextConfig config = new FullTextConfig();
        config.setEnableNumbersSearch(annotation.enableNumbersSearch());
        config.setEnableKbLayout(annotation.enableKbLayout());
        config.setEnableNumbersSearch(annotation.enableNumbersSearch());
        config.setEnableTranslit(annotation.enableTranslit());
        config.setEnableWarmupOnNsCopy(annotation.enableWarmupOnNsCopy());
        config.setBm25Boost(annotation.bm25Boost());
        config.setBm25Weight(annotation.bm25Weight());
        config.setPositionBoost(annotation.positionBoost());
        config.setPositionWeight(annotation.positionWeight());
        config.setDistanceBoost(annotation.distanceBoost());
        config.setDistanceWeight(annotation.distanceWeight());
        config.setFullMatchBoost(annotation.fullMatchBoost());
        config.setMinRelevancy(annotation.minRelevancy());
        config.setTermLenBoost(annotation.termLenBoost());
        config.setTermLenWeight(annotation.termLenWeight());
        config.setSumRanksByFieldsRatio(annotation.sumRanksByFieldsRatio());
        config.setLogLevel(annotation.logLevel());
        config.setMaxRebuildSteps(annotation.maxRebuildSteps());
        config.setMaxStepSize(annotation.maxStepSize());
        config.setMaxTypos(annotation.maxTypos());
        config.setMaxTypoLen(annotation.maxTypoLen());
        config.setMergeLimit(annotation.mergeLimit());
        config.setPartialMatchDecrease(annotation.partialMatchDecrease());
        config.setStemmers(Arrays.asList(annotation.stemmers()));
        config.setStopWords(Arrays.asList(annotation.stopWords()));
        config.setSynonyms(getSynonymsList(annotation.synonyms()));
        config.setExtraWordSymbols(annotation.extraWordSymbols());
        return config;
    }

    private static List<Synonym> getSynonymsList(FullText.Synonym[] synonyms) {
        return Arrays.stream(synonyms)
                .map(a -> new Synonym(a.tokens(), a.alternatives()))
                .collect(Collectors.toList());
    }
}
