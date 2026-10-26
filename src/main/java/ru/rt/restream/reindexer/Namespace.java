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

import java.util.Optional;

/**
 * Provides methods for manipulating Reindexer namespace data.
 *
 * @param <T> the type of stored items
 */
public interface Namespace<T> {

    /**
     * Begin a unit of work and return the associated namespace Transaction object.
     *
     * @return a Transaction instance
     */
    Transaction<T> beginTransaction();

    /**
     * Inserts the given item data.
     *
     * @param item the item data
     * @return the inserted {@link T} item
     */
    T insert(T item);

    /**
     * Inserts the given json-formatted item data.
     * Returns the server-generated {@link T} item, which may contain fields not present
     * in the input JSON, or {@link Optional#empty()} if the server returns no data.
     *
     * @param item the json-formatted item data
     * @return the server-generated {@link T} item, or {@link Optional#empty()} if no data was returned
     */
    Optional<T> insert(String item);

    /**
     * Inserts or updates the given item data.
     *
     * @param item the item data
     * @return the upserted {@link T} item
     */
    T upsert(T item);

    /**
     * Inserts or updates the given json-formatted item data.
     * Returns the server-generated {@link T} item, which may contain fields not present
     * in the input JSON, or {@link Optional#empty()} if the server returns no data.
     *
     * @param item the json-formatted item data
     * @return the server-generated {@link T} item, or {@link Optional#empty()} if no data was returned
     */
    Optional<T> upsert(String item);

    /**
     * Updates the given item data.
     *
     * @param item the item data
     * @return the updated {@link T} item
     */
    T update(T item);

    /**
     * Updates the given json-formatted item data.
     * Returns the server-generated {@link T} item, which may contain fields not present
     * in the input JSON, or {@link Optional#empty()} if the server returns no data.
     *
     * @param item the json-formatted item data
     * @return the server-generated {@link T} item, or {@link Optional#empty()} if no data was returned
     */
    Optional<T> update(String item);

    /**
     * Deletes the given item data.
     *
     * @param item          the item data
     */
    void delete(T item);

    /**
     * Deletes the given json-formatted item data.
     *
     * @param item          the json-formatted item data
     */
    void delete(String item);

    /**
     * Creates new Query for building request
     *
     * @return builder for building request
     */
    Query<T> query();

    /**
     * Associates the specified value with the specified key in reindexer namespace.
     *
     * @param key  key with which the specified value is to be associated
     * @param data value to be associated with the specified key
     */
    void putMeta(String key, String data);

    /**
     * Returns the value to which the specified key is mapped, or empty string if namespace contains no mapping for the
     * key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or empty string if namespace contains no mapping for the
     * key
     */
    String getMeta(String key);

    /**
     * Executes the given SQL query and returns a {@link ResultIterator}.
     *
     * @param query the SQL query to execute
     * @return the {@link ResultIterator} to use
     */
    ResultIterator<T> execSql(String query);

    /**
     * Executes the given SQL update query.
     *
     * @param query the SQL update query to execute
     */
    void updateSql(String query);

}
