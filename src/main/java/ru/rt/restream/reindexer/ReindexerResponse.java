/*
 * Copyright 2020 Restream
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.binding.Consts;

/**
 * Contains reindexer server response.
 */
public class ReindexerResponse {

    private final int code;

    private final String errorMessage;

    private final Object[] arguments;

    /**
     * Creates new instance.
     *
     * @param code         if the value is different from '0' - the answer contains an error.
     * @param errorMessage reindexer server error message.
     * @param arguments    response arguments
     */
    public ReindexerResponse(int code, String errorMessage, Object[] arguments) {
        this.code = code;
        this.errorMessage = errorMessage;
        this.arguments = arguments;
    }

    /**
     * Check if the current response contains an error.
     *
     * @return true, if reindexer response contains an error
     */
    public boolean hasError() {
        return code != Consts.ERR_OK;
    }

    /**
     * Get the current response code.
     *
     * @return the current response code
     */
    public int getCode() {
        return code;
    }

    /**
     * Get the current response error message.
     *
     * @return the current response error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Get the current response arguments.
     *
     * @return the current response arguments
     */
    public Object[] getArguments() {
        return arguments;
    }
}
