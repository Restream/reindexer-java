package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.binding.cproto.Cproto;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;

/**
 * Represents approach for bootstrapping Reindexer.
 */
public final class Configuration {

    private String url;

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
                return new Reindexer(new Cproto(url));
            case "http":
            case "builtin":
            case "builtinserver":
                throw new UnimplementedException();
            default:
                throw new IllegalArgumentException();
        }
    }

}
