package ru.rt.restream.reindexer.exceptions;

import ru.rt.restream.reindexer.binding.Consts;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ReindexerExceptionFactory {

    private static final Map<Integer, Function<Integer, ReindexerException>> FACTORIES = new HashMap<>();

    public ReindexerExceptionFactory() {
        FACTORIES.put(Consts.ERR_CONFLICT, code -> new IndexConflictException());
    }

    public static ReindexerException fromErrorCode(int code) {
        return FACTORIES.getOrDefault(code, c -> new ReindexerException())
                .apply(code);
    }

}
