package ru.rt.restream.reindexer;

/**
 * An object that represents a deferred result for asynchronous request processing.
 */
public class DeferredResult<T> {

    /**
     * Asynchronous request result.
     */
    private T item;

    /**
     * Asynchronous request error.
     */
    private Exception error;

    public T getItem() {
        return item;
    }

    public void setItem(T item) {
        this.item = item;
    }

    public Exception getError() {
        return error;
    }

    public void setError(Exception error) {
        this.error = error;
    }

    public boolean hasError() {
        return error != null;
    }

}
