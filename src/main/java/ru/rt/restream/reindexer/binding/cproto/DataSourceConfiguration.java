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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

    private DataSourceConfiguration(List<String> urls, boolean allowUnlistedDataSource) {
        this.urls = urls;
        this.allowUnlistedDataSource = allowUnlistedDataSource;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Return a list of reindexer database urls of the form protocol://host:port/database_name.
     */
    public List<String> getUrls() {
        return urls;
    }

    public List<URI> getUris() {
        return urls.stream()
                .map(URI::create)
                .collect(Collectors.toList());
    }

    public boolean isAllowUnlistedDataSource() {
        return allowUnlistedDataSource;
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
            this.urls = configuration.urls;
            this.allowUnlistedDataSource = configuration.allowUnlistedDataSource;
        }

        /**
         * Configure reindexer database url.
         *
         * @param url a database url of the form protocol://host:port/database_name
         * @return the {@link Builder} for further customizations
         */
        public Builder addUrl(String url) {
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
         * Allow to use database urls from #replicationstats which are not in the list of urls.
         *
         * @param allow enable permission to use unlisted urls
         * @return the {@link Builder} for further customizations
         */
        public Builder allowUnlistedDataSource(boolean allow) {
            this.allowUnlistedDataSource = allow;
            return this;
        }

        /**
         * Build and return a {@link DataSource} configuration.
         *
         * @return a {@link DataSourceConfiguration}
         */
        public DataSourceConfiguration build() {
            return new DataSourceConfiguration(urls, allowUnlistedDataSource);
        }

    }

}
