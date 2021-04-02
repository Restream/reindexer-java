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
package ru.rt.restream.reindexer;

import java.util.List;

/**
 * Contains results of aggregations.
 */
public class AggregationResult {

    private List<String> fields;

    private String type;

    private double value;

    private List<Facet> facets;

    List<String> distincts;

    /**
     * Contains aggregation facet group.
     */
    public static class Facet {
        private List<String> values;
        private int count;

        /**
         * Get the current facet values.
         *
         * @return the current facet values
         */
        public List<String> getValues() {
            return values;
        }

        /**
         * Set the current facet values.
         *
         * @param values facet values
         */
        public void setValues(List<String> values) {
            this.values = values;
        }

        /**
         * Get the current facet count.
         *
         * @return the current facet count
         */
        public int getCount() {
            return count;
        }

        /**
         * Set the current facet count.
         *
         * @param count facet count
         */
        public void setCount(int count) {
            this.count = count;
        }
    }

    /**
     * Get the current aggregation result fields.
     *
     * @return the current aggregation result fields
     */
    public List<String> getFields() {
        return fields;
    }

    /**
     * Set the current aggregation result fields.
     *
     * @param fields aggregation result fields
     */
    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    /**
     * Get the current aggregation type. Possible values - min, max, sum, avg, facet, distinct.
     *
     * @return the current aggregation result type
     */
    public String getType() {
        return type;
    }

    /**
     * Set the current aggregation type. Possible values - min, max, sum, avg, facet, distinct.
     *
     * @param type the aggregation result type.
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Get the current aggregation result value.
     *
     * @return the current aggregation result value
     */
    public double getValue() {
        return value;
    }

    /**
     * Set the current aggregation result value.
     *
     * @param value aggregation result value
     */
    public void setValue(double value) {
        this.value = value;
    }

    /**
     * Get the current facet aggregation result.
     *
     * @return the current facet aggregation result
     */
    public List<Facet> getFacets() {
        return facets;
    }

    /**
     * Set the current facet aggregation result facets.
     *
     * @param facets aggregation result facets
     */
    public void setFacets(List<Facet> facets) {
        this.facets = facets;
    }

    /**
     * Get distinct aggregation results.
     *
     * @return the current distinct aggregation result values
     */
    public List<String> getDistincts() {
        return distincts;
    }

    /**
     * Set distinct aggregation results.
     *
     * @param distincts result of distinct aggregation result
     */
    public void setDistincts(List<String> distincts) {
        this.distincts = distincts;
    }
}
