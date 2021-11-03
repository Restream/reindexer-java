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

package ru.rt.restream.reindexer.binding.cproto;

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A {@link DataSource} configuration.
 */
public class DataSourceConfiguration {

    /**
     * A list of reindexer database urls of the form protocol://host:port/database_name.
     */
    private final List<String> urls;

    /**
     * A permission to use database urls from #replicationstats which are not in the list of urls.
     */
    private final boolean allowUnlistedDataSource;

    /**
     * An index of the current active data source.
     */
    private final MutableInt active;

    private DataSourceConfiguration(Builder builder) {
        urls = builder.urls;
        allowUnlistedDataSource = builder.allowUnlistedDataSource;
        active = builder.active;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Return a list of reindexer database urls of the form protocol://host:port/database_name.
     *
     * @return a list of reindexer database urls of the form protocol://host:port/database_name
     */
    public List<String> getUrls() {
        return urls;
    }

    public boolean isAllowUnlistedDataSource() {
        return allowUnlistedDataSource;
    }

    /**
     * Returns the index of the current active data source.
     *
     * @return the index of the current active data source
     */
    public int getActive() {
        return active.getValue();
    }

    /**
     * Sets the index of the current active data source.
     *
     * @param value the index of the current active data source
     */
    public void setActive(int value) {
        active.setValue(value);
    }

    /**
     * Builder for a {@link DataSource} configuration.
     */
    public static class Builder {

        /**
         * A list of reindexer database urls database url of the form protocol://host:port/database_name.
         */
        private List<String> urls = new ArrayList<>();

        /**
         * A permission to use database urls from #replicationstats which are not in the list of urls.
         */
        private boolean allowUnlistedDataSource = true;

        /**
         * An index of the current active data source.
         */
        private MutableInt active = new MutableInt(0);

        /**
         * Private constructor with default values for use in the method builder() only.
         */
        private Builder() {
        }

        /**
         * Private constructor for use in the method toBuilder() only.
         *
         * @param configuration parent {@link DataSourceConfiguration}
         */
        private Builder(DataSourceConfiguration configuration) {
            urls = configuration.urls;
            allowUnlistedDataSource = configuration.allowUnlistedDataSource;
            active = configuration.active;
        }

        /**
         * Configure reindexer database url.
         *
         * @param url a database url of the form protocol://host:port/database_name
         * @return the {@link Builder} for further customizations
         */
        public Builder url(String url) {
            urls.add(Objects.requireNonNull(url));
            return this;
        }

        /**
         * Configure reindexer database urls.
         *
         * @param urls a list of database url of the form protocol://host:port/database_name
         * @return the {@link Builder} for further customizations
         */
        public Builder urls(List<String> urls) {
            this.urls = Objects.requireNonNull(urls);
            return this;
        }

        /**
         * Allows usage of the database urls from #replicationstats which are not in the list of urls.
         *
         * @param allowUnlistedDataSource enable permission to use unlisted urls
         * @return the {@link Builder} for further customizations
         */
        public Builder allowUnlistedDataSource(boolean allowUnlistedDataSource) {
            this.allowUnlistedDataSource = allowUnlistedDataSource;
            return this;
        }

        /**
         * Build and return a {@link DataSource} configuration.
         *
         * @return a {@link DataSourceConfiguration}
         */
        public DataSourceConfiguration build() {
            return new DataSourceConfiguration(this);
        }

    }

}
