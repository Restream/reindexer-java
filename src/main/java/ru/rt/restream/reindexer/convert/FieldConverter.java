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
package ru.rt.restream.reindexer.convert;

/**
 * An interface that can be implemented to convert field value between the Reindexer stored database type: 
 * {@link ru.rt.restream.reindexer.FieldType}, and the one used within the POJO representation.
 * @param <X> the field type used within the POJO
 * @param <Y> the Reindexer stored database type
 */
public interface FieldConverter<X, Y> {

    /**
     * Converts Reindexer stored database type value to the one used within the POJO representation.
     * @param dbData the Reindexer stored database type value to convert
     * @return the type value used within the POJO representation    
     */
    X convertToFieldType(Y dbData);

    /**
     * Converts a POJO field type value to the one stored in Reindexer database.
     * @param field the field type value used within the POJO
     * @return the Reindexer stored database type value
     */
    Y convertToDatabaseType(X field);
}
