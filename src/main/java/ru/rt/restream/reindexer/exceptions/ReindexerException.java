package ru.rt.restream.reindexer.exceptions;

public class ReindexerException extends RuntimeException {

    public ReindexerException() {
    }

    public ReindexerException(String message) {
        super(message);
    }


    public ReindexerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReindexerException(Throwable cause) {
        super(cause);
    }

}
