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

import ru.rt.restream.reindexer.binding.builtin.Builtin;
import ru.rt.restream.reindexer.binding.cproto.Cproto;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;

import java.time.Duration;

/**
 * Represents approach for bootstrapping Reindexer.
 */
public final class Configuration {

    private static final int DEFAULT_CONNECTION_POOL_SIZE = 8;

    private String url;

    private int connectionPoolSize = DEFAULT_CONNECTION_POOL_SIZE;

    private Duration requestTimeout = Duration.ofSeconds(60L);

    private Configuration() {

    }

    public static Configuration builder() {
        return new Configuration();
    }

    /**
     * Configure reindexer database url.
     *
     * @param url a database url of the form protocol://host:port/database_name
     * @return the {@link Configuration} for further customizations
     */
    public Configuration url(String url) {
        this.url = url;
        return this;
    }

    /**
     * Configure reindexer connection pool size. Defaults to 8.
     *
     * @param connectionPoolSize the connection pool size
     * @return the {@link Configuration} for further customizations
     */
    public Configuration connectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    /**
     * Configure reindexer request timeout. Defaults to 60 seconds.
     *
     * @param requestTimeout the request timeout
     * @return the {@link Configuration} for further customizations
     */
    public Configuration requestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    /**
     * Build and return reindexer connector instance.
     *
     * @return configured reindexer connector instance
     */
    public Reindexer getReindexer() {
        if (url == null) {
            throw new IllegalStateException("Url is not configured");
        }

        String protocol = url.substring(0, url.indexOf(":"));
        switch (protocol) {
            case "cproto":
                return new Reindexer(new Cproto(url, connectionPoolSize, requestTimeout));
            case "builtin":
                return new Reindexer(new Builtin(url, requestTimeout));
            case "http":
            case "builtinserver":
                throw new UnimplementedException("Protocol: '" + protocol + "' is not suppored");
            default:
                throw new IllegalArgumentException();
        }
    }

}
