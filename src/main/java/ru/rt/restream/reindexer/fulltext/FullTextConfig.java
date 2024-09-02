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

import ru.rt.restream.reindexer.annotations.FullText;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
public class FullTextConfig {

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

    /**
     * For use in method of() only.
     * Don't make public the constructor!
     */
    private FullTextConfig() {
    }


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
        config.setStemmers(annotation.stemmers());
        config.setStopWords(annotation.stopWords());
        config.setSynonyms(getSynonymsList(annotation.synonyms()));
        config.setExtraWordSymbols(annotation.extraWordSymbols());
        return config;
    }

    private static List<Synonym> getSynonymsList(FullText.Synonym[] synonyms) {
        return Arrays.stream(synonyms)
                .map(a -> new Synonym(a.tokens(), a.alternatives()))
                .collect(Collectors.toList());
    }

    public boolean isEnableKbLayout() {
        return enableKbLayout;
    }

    public void setEnableKbLayout(boolean enableKbLayout) {
        this.enableKbLayout = enableKbLayout;
    }

    public boolean isEnableNumbersSearch() {
        return enableNumbersSearch;
    }

    public void setEnableNumbersSearch(boolean enableNumbersSearch) {
        this.enableNumbersSearch = enableNumbersSearch;
    }

    public boolean isEnableTranslit() {
        return enableTranslit;
    }

    public void setEnableTranslit(boolean enableTranslit) {
        this.enableTranslit = enableTranslit;
    }

    public boolean isEnableWarmupOnNsCopy() {
        return enableWarmupOnNsCopy;
    }

    public void setEnableWarmupOnNsCopy(boolean enableWarmupOnNsCopy) {
        this.enableWarmupOnNsCopy = enableWarmupOnNsCopy;
    }

    public double getBm25Boost() {
        return bm25Boost;
    }

    public void setBm25Boost(double bm25Boost) {
        this.bm25Boost = bm25Boost;
    }

    public double getBm25Weight() {
        return bm25Weight;
    }

    public void setBm25Weight(double bm25Weight) {
        this.bm25Weight = bm25Weight;
    }

    public double getPositionBoost() {
        return positionBoost;
    }

    public void setPositionBoost(double positionBoost) {
        this.positionBoost = positionBoost;
    }

    public double getPositionWeight() {
        return positionWeight;
    }

    public void setPositionWeight(double positionWeight) {
        this.positionWeight = positionWeight;
    }

    public double getDistanceBoost() {
        return distanceBoost;
    }

    public void setDistanceBoost(double distanceBoost) {
        this.distanceBoost = distanceBoost;
    }

    public double getDistanceWeight() {
        return distanceWeight;
    }

    public void setDistanceWeight(double distanceWeight) {
        this.distanceWeight = distanceWeight;
    }

    public double getFullMatchBoost() {
        return fullMatchBoost;
    }

    public void setFullMatchBoost(double fullMatchBoost) {
        this.fullMatchBoost = fullMatchBoost;
    }

    public double getMinRelevancy() {
        return minRelevancy;
    }

    public void setMinRelevancy(double minRelevancy) {
        this.minRelevancy = minRelevancy;
    }

    public double getSumRanksByFieldsRatio() {
        return sumRanksByFieldsRatio;
    }

    public void setSumRanksByFieldsRatio(double sumRanksByFieldsRatio) {
        this.sumRanksByFieldsRatio = sumRanksByFieldsRatio;
    }

    public double getTermLenBoost() {
        return termLenBoost;
    }

    public void setTermLenBoost(double termLenBoost) {
        this.termLenBoost = termLenBoost;
    }

    public double getTermLenWeight() {
        return termLenWeight;
    }

    public void setTermLenWeight(double termLenWeight) {
        this.termLenWeight = termLenWeight;
    }

    public int getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(int logLevel) {
        this.logLevel = logLevel;
    }

    public int getMaxRebuildSteps() {
        return maxRebuildSteps;
    }

    public void setMaxRebuildSteps(int maxRebuildSteps) {
        this.maxRebuildSteps = maxRebuildSteps;
    }

    public int getMaxStepSize() {
        return maxStepSize;
    }

    public void setMaxStepSize(int maxStepSize) {
        this.maxStepSize = maxStepSize;
    }

    public int getMaxTypoLen() {
        return maxTypoLen;
    }

    public void setMaxTypoLen(int maxTypoLen) {
        this.maxTypoLen = maxTypoLen;
    }

    public int getMaxTypos() {
        return maxTypos;
    }

    public void setMaxTypos(int maxTypos) {
        this.maxTypos = maxTypos;
    }

    public int getMergeLimit() {
        return mergeLimit;
    }

    public void setMergeLimit(int mergeLimit) {
        this.mergeLimit = mergeLimit;
    }

    public int getPartialMatchDecrease() {
        return partialMatchDecrease;
    }

    public void setPartialMatchDecrease(int partialMatchDecrease) {
        this.partialMatchDecrease = partialMatchDecrease;
    }

    public List<String> getStemmers() {
        return stemmers;
    }

    public void setStemmers(List<String> stemmers) {
        this.stemmers = stemmers;
    }

    public void setStemmers(String... stemmers) {
        this.stemmers = Arrays.asList(stemmers);
    }

    public List<String> getStopWords() {
        return stopWords;
    }

    public void setStopWords(List<String> stopWords) {
        this.stopWords = stopWords;
    }

    public void setStopWords(String... stopWords) {
        this.stopWords = Arrays.asList(stopWords);
    }

    public List<Synonym> getSynonyms() {
        return synonyms;
    }

    public void setSynonyms(List<Synonym> synonyms) {
        this.synonyms = synonyms;
    }

    public String getExtraWordSymbols() {
        return extraWordSymbols;
    }

    public void setExtraWordSymbols(String extraWordSymbols) {
        this.extraWordSymbols = extraWordSymbols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FullTextConfig that = (FullTextConfig) o;
        return enableKbLayout == that.enableKbLayout
                && enableNumbersSearch == that.enableNumbersSearch
                && enableTranslit == that.enableTranslit
                && enableWarmupOnNsCopy == that.enableWarmupOnNsCopy
                && Double.compare(that.bm25Boost, bm25Boost) == 0
                && Double.compare(that.bm25Weight, bm25Weight) == 0
                && Double.compare(that.positionBoost, positionBoost) == 0
                && Double.compare(that.positionWeight, positionWeight) == 0
                && Double.compare(that.distanceBoost, distanceBoost) == 0
                && Double.compare(that.distanceWeight, distanceWeight) == 0
                && Double.compare(that.termLenBoost, termLenBoost) == 0
                && Double.compare(that.termLenWeight, termLenWeight) == 0
                && Double.compare(that.fullMatchBoost, fullMatchBoost) == 0
                && Double.compare(that.minRelevancy, minRelevancy) == 0
                && Double.compare(that.sumRanksByFieldsRatio, sumRanksByFieldsRatio) == 0
                && logLevel == that.logLevel
                && mergeLimit == that.mergeLimit
                && maxTypos == that.maxTypos
                && maxTypoLen == that.maxTypoLen
                && maxRebuildSteps == that.maxRebuildSteps
                && maxStepSize == that.maxStepSize
                && partialMatchDecrease == that.partialMatchDecrease
                && Objects.equals(stemmers, that.stemmers)
                && Objects.equals(stopWords, that.stopWords)
                && Objects.equals(synonyms, that.synonyms)
                && Objects.equals(extraWordSymbols, that.extraWordSymbols);
    }
}
