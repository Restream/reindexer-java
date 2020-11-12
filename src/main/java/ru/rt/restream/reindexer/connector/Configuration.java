package ru.rt.restream.reindexer.connector;

import ru.rt.restream.reindexer.connector.binding.cproto.Cproto;
import ru.rt.restream.reindexer.connector.exceptions.UnimplementedException;

public final class Configuration {

    private String url;

    private Configuration() {

    }

    public static Configuration builder() {
        return new Configuration();
    }

    public Configuration url(String url) {
        this.url = url;
        return this;
    }

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
