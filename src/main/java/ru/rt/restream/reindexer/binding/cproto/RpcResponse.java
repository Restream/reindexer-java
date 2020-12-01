package ru.rt.restream.reindexer.binding.cproto;

import ru.rt.restream.reindexer.binding.Consts;

public class RpcResponse {

    private final int code;

    private final String errorMessage;

    private final Object[] arguments;

    public RpcResponse(int code, String errorMessage, Object[] arguments) {
        this.code = code;
        this.errorMessage = errorMessage;
        this.arguments = arguments;
    }

    public boolean hasError() {
        return code != Consts.ERR_OK;
    }

    public int getCode() {
        return code;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Object[] getArguments() {
        return arguments;
    }
}
