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
package ru.rt.restream.reindexer.exceptions;

/**
 * Common reindexer exception.
 */
public class ReindexerException extends RuntimeException {

    /**
     * Creates new instance from an error message.
     *
     * @param message the error message
     */
    public ReindexerException(String message) {
        super(message);
    }

    /**
     * Creates new instance from an error message and specified cause.
     *
     * @param message the error message
     * @param cause   the cause of error
     */
    public ReindexerException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates new instance with the specified cause.
     *
     * @param cause the cause of error
     */
    public ReindexerException(Throwable cause) {
        super(cause);
    }

}
