/**
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
package ru.rt.restream.reindexer.binding;

/**
 * A transaction context.
 */
public interface TransactionContext {

    /**
     * Modifies the item data in the transaction that is associated with the context.
     *
     * @param data       item data
     * @param mode       modify mode (INSERT, UPDATE, UPSERT, DELETE)
     * @param precepts   precepts (i.e. "id=serial()", "updated_at=now()")
     * @param stateToken state token
     */
    void modifyItem(byte[] data, int mode, String[] precepts, int stateToken);

    /**
     * Invoke select query.
     *
     * @param queryData  encoded query data (selected indexes, predicates, etc)
     * @param fetchCount items count to fetch within a query request
     * @param ptVersions payload type state tokens
     */
    RequestContext selectQuery(byte[] queryData, int fetchCount, long[] ptVersions);

    /**
     * Invoke update query in the transaction that is associated with the context.
     *
     * @param queryData encoded query data (selected indexes, predicates, etc)
     */
    void updateQuery(byte[] queryData);

    /**
     * Invoke delete query in the transaction that is associated with the context.
     *
     * @param queryData encoded query data (selected indexes, predicates, etc)
     */
    void deleteQuery(byte[] queryData);

    /**
     * Commits the transaction that is associated with the context.
     */
    void commit();

    /**
     * Rollbacks the transaction that is associated with the context.
     */
    void rollback();

    /**
     * Closes context resources.
     */
    void close();

}
