package ru.rt.restream.reindexer.binding.cproto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import ru.rt.restream.reindexer.binding.Consts;

@Getter
@Setter
@RequiredArgsConstructor
public class RpcResponse {

    private final int code;

    private final String errorMessage;

    private final Object[] arguments;

    public boolean hasError() {
        return code != Consts.ERR_OK;
    }

}
