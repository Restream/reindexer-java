package ru.rt.restream.reindexer.connector.binding.cproto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import ru.rt.restream.reindexer.connector.binding.Consts;

import java.util.List;

@Getter
@AllArgsConstructor
public class RpcResult {

    private final Error error;

    private final List<Object> arguments;

    @Getter
    @AllArgsConstructor
    public static class Error {
        public int code;
        public String message;

        public boolean isOk() {
            return code == Consts.ERR_OK;
        }
    }

}
