package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.binding.cproto.Cproto;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;

/**
 * Represents approach for bootstrapping Reindexer.
 */
public final class Configuration {

    private static final int DEFAULT_CONNECTION_POOL_SIZE = 8;

    private static final long DEFAULT_CONNECTION_TIMEOUT = 60L;

    private String url;

    private int connectionPoolSize = DEFAULT_CONNECTION_POOL_SIZE;

    private long connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    private Configuration() {

    }

    public static Configuration builder() {
        return new Configuration();
    }

    /**
     * Configure reindexer database url.
     *
     * @param url a database url of the form protocol://host:port/database_name
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
     * Configure reindexer connection timeout. Defaults to 60 seconds.
     *
     * @param connectionTimeout the connection timeout in seconds
     * @return the {@link Configuration} for further customizations
     */
    public Configuration connectionTimeout(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
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
                return new Reindexer(new Cproto(url, connectionPoolSize, connectionTimeout));
            case "http":
            case "builtin":
            case "builtinserver":
                throw new UnimplementedException();
            default:
                throw new IllegalArgumentException();
        }
    }

}
