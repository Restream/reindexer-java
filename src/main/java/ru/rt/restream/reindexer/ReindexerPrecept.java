/*
 * Copyright 2020-present Restream
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

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import ru.rt.restream.reindexer.expression.PreceptFunction;

/**
 * Precept is a special reindexer embedded function, such as {@code serial()}, {@code now()}.
 *
 * @see ru.rt.restream.reindexer.annotations.Serial
 */
@Getter
@Builder
@ToString
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ReindexerPrecept {

    /**
     * A field path to apply precept to.
     */
    @NonNull
    private final String fieldPath;

    /**
     * A JSON path to apply precept to.
     */
    @NonNull
    private final String jsonPath;

    /**
     * A precept expression.
     */
    @NonNull
    private final PreceptFunction expression;

    /**
     * A precept apply strategy. Default is {@link PreceptApplyStrategy#ALWAYS}.
     */
    @NonNull
    @Builder.Default
    private final PreceptApplyStrategy strategy = PreceptApplyStrategy.ALWAYS;

}
