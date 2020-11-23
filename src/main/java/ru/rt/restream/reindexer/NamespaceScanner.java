package ru.rt.restream.reindexer;

/**
 * Scans item class for reindexer namespace configuration.
 */
public interface NamespaceScanner {

    /**
     * Returns {@link Namespace} for the given namespace and item class.
     *
     * @param namespaceName namespace name to get
     * @param itemClass     item class to store within specified namespace
     */
    <T> Namespace<T> scanClassNamespace(String namespaceName, Class<T> itemClass);

}
