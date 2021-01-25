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

package ru.rt.restream.reindexer.util;

import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

/**
 * Utility class for native calls.
 */
public class NativeUtils {

    /**
     * Returns the {@link ByteBuffer} from the native memory.
     *
     * @param resultsPtr the results pointer
     * @param cptr       the item pointer from results
     * @param nsId       the namespace id
     * @return the {@link ByteBuffer} to use
     */
    public static ByteBuffer getNativeBuffer(long resultsPtr, long cptr, int nsId) {
        byte[] bytes = getBytes(resultsPtr, cptr, nsId);
        return new ByteBuffer(bytes).rewind();
    }

    private static native byte[] getBytes(long resultsPtr, long cptr, int nsId);

    /**
     * Frees the buffer from the native memory.
     *
     * @param resultsPtr the results pointer
     */
    public static native void freeNativeBuffer(long resultsPtr);

}
