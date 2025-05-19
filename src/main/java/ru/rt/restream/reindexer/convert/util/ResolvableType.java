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
package ru.rt.restream.reindexer.convert.util;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Represents a resolved type, this includes {@code type} information,
 * {@code componentType} and {@code isCollectionLike} if the {@code type} is either
 * assignable of {@code java.util.Collection} or an array.
 */
@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public final class ResolvableType {
    private final Class<?> type;
    private final Class<?> componentType;
    private final boolean isCollectionLike;
}
