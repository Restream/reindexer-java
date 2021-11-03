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

import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.builtin.Builtin;
import ru.rt.restream.reindexer.binding.builtin.server.BuiltinServer;
import ru.rt.restream.reindexer.binding.cproto.Cproto;
import ru.rt.restream.reindexer.binding.cproto.DataSourceConfiguration;
import ru.rt.restream.reindexer.binding.cproto.DataSourceFactory;
import ru.rt.restream.reindexer.binding.cproto.DataSourceFactoryStrategy;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents approach for bootstrapping Reindexer.
 */
public final class ReindexerConfiguration {

    private static final int DEFAULT_CONNECTION_POOL_SIZE = 8;

    private final List<String> urls = new ArrayList<>();

    private boolean allowUnlistedDataSource = false;

    private DataSourceFactory dataSourceFactory = DataSourceFactoryStrategy.NEXT;

    private int connectionPoolSize = DEFAULT_CONNECTION_POOL_SIZE;

    private Duration requestTimeout = Duration.ofSeconds(60L);

    private Duration serverStartupTimeout = Duration.ofMinutes(3L);

    private String serverConfigFile = "default-builtin-server-config.yml";

    private ReindexerConfiguration() {

    }

    public static ReindexerConfiguration builder() {
        return new ReindexerConfiguration();
    }

    /**
     * Configure reindexer database url.
     *
     * @param url a database url of the form protocol://host:port/database_name
     * @return the {@link ReindexerConfiguration} for further customizations
     */
    public ReindexerConfiguration url(String url) {
        urls.add(url);
        return this;
    }

    /**
     * Configure reindexer database urls.
     *
     * @param urls a list of database url of the form protocol://host:port/database_name
     * @return the {@link ReindexerConfiguration} for further customizations
     */
    public ReindexerConfiguration urls(List<String> urls) {
        this.urls.addAll(Objects.requireNonNull(urls));
        return this;
    }

    /**
     * Allows usage of the database urls from #replicationstats which are not in the list of urls.
     *
     * @param allowUnlistedDataSource enable permission to use unlisted urls
     * @return the {@link ReindexerConfiguration} for further customizations
     */
    public ReindexerConfiguration allowUnlistedDataSource(boolean allowUnlistedDataSource) {
        this.allowUnlistedDataSource = allowUnlistedDataSource;
        return this;
    }

    /**
     * Configure a {@link DataSourceFactory}. Defaults to {@link DataSourceFactoryStrategy#NEXT}.
     *
     * @param dataSourceFactory the {@link DataSourceFactory} to use
     * @return the {@link ReindexerConfiguration} for further customizations
     */
    public ReindexerConfiguration dataSourceFactory(DataSourceFactory dataSourceFactory) {
        this.dataSourceFactory = dataSourceFactory;
        return this;
    }

    /**
     * Configure reindexer connection pool size. Defaults to 8.
     *
     * @param connectionPoolSize the connection pool size
     * @return the {@link ReindexerConfiguration} for further customizations
     */
    public ReindexerConfiguration connectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    /**
     * Configure reindexer request timeout. Defaults to 60 seconds.
     *
     * @param requestTimeout the request timeout
     * @return the {@link ReindexerConfiguration} for further customizations
     */
    public ReindexerConfiguration requestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    /**
     * Configure reindexer server startup timeout. Defaults to 3 minutes.
     *
     * @param serverStartupTimeout the server startup timeout
     * @return the {@link ReindexerConfiguration} for further customizations
     */
    public ReindexerConfiguration serverStartupTimeout(Duration serverStartupTimeout) {
        this.serverStartupTimeout = serverStartupTimeout;
        return this;
    }

    /**
     * Configure reindexer server config file. Defaults to "default-builtin-server-config.yml".
     *
     * @param serverConfigFile the server config file
     * @return the {@link ReindexerConfiguration} for further customizations
     */
    public ReindexerConfiguration serverConfigFile(String serverConfigFile) {
        this.serverConfigFile = serverConfigFile;
        return this;
    }

    /**
     * Build and return reindexer connector instance.
     *
     * @return configured reindexer connector instance
     */
    public Reindexer getReindexer() {
        if (urls.isEmpty()) {
            throw new IllegalStateException("Url is not configured");
        }
        String protocol = null;
        List<URI> uris = new ArrayList<>();
        for (String url : urls) {
            URI uri = URI.create(url);
            if (protocol == null) {
                protocol = uri.getScheme();
            } else if (!protocol.equals(uri.getScheme())) {
                throw new IllegalArgumentException("Protocol must be the same for all DSNs");
            }
            uris.add(uri);
        }
        return new Reindexer(getBinding(protocol, uris));
    }

    private Binding getBinding(String protocol, List<URI> uris) {
        switch (protocol) {
            case "cproto":
                DataSourceConfiguration dataSourceConfig = DataSourceConfiguration.builder()
                        .urls(urls)
                        .allowUnlistedDataSource(allowUnlistedDataSource)
                        .build();
                return new Cproto(dataSourceFactory, dataSourceConfig, connectionPoolSize, requestTimeout);
            case "builtin":
                return new Builtin(uris.get(0), requestTimeout);
            case "builtinserver":
                return new BuiltinServer(uris.get(0), serverConfigFile, serverStartupTimeout, requestTimeout);
            default:
                throw new UnimplementedException("Protocol: '" + protocol + "' is not suppored");
        }
    }

}
