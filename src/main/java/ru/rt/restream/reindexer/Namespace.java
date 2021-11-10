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
     * @param item          the item data
     */
    void insert(T item);

    /**
     * Inserts the given json-formatted item data
     *
     * @param item          the json-formatted item data
     */
    void insert(String item);

    /**
     * Inserts or updates the given item data.
     *
     * @param item          the item data
     */
    void upsert(T item);

    /**
     * Inserts or updates the given json-formatted item data.
     *
     * @param item          the json-formatted item data
     */
    void upsert(String item);

    /**
     * Updates the given item data.
     *
     * @param item          the item data
     */
    void update(T item);

    /**
     * Updates the given json-formatted item data.
     *
     * @param item          the json-formatted item data
     */
    void update(String item);

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
     * Executes the given SQL query and returns a {@link CloseableIterator}.
     *
     * @param query the SQL query to execute
     * @return the {@link CloseableIterator} to use
     */
    CloseableIterator<T> execSql(String query);

    /**
     * Executes the given SQL update query.
     *
     * @param query the SQL update query to execute
     */
    void updateSql(String query);

}
