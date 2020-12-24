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

import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;

/**
 * Binding to Reindexer instance.
 */
public interface Binding {

    int PING = 0;

    int LOGIN = 1;

    int OPEN_DATABASE = 2;

    int CLOSE_DATABASE = 3;

    int DROP_DATABASE = 4;

    int OPEN_NAMESPACE = 16;

    int CLOSE_NAMESPACE = 17;

    int DROP_NAMESPACE = 18;

    int ADD_INDEX = 21;

    int ENUM_NAMESPACES = 22;

    int DROP_INDEX = 24;

    int UPDATE_INDEX = 25;

    int START_TRANSACTION = 29;

    int MODIFY_ITEM = 33;

    int DELETE_QUERY = 34;

    int UPDATE_QUERY = 35;

    int SELECT = 48;

    int SELECT_SQL = 49;

    int FETCH_RESULTS = 50;

    int CLOSE_RESULTS = 51;

    int GET_META = 64;

    int PUT_META = 65;

    int ENUM_META = 66;

    int CODE_MAX = 128;

    int RESULTS_FORMAT_MASK = 0xF;

    int RESULTS_PURE = 0x0;

    int RESULTS_PTRS = 0x1;

    int RESULTS_C_JSON = 0x2;

    int RESULTS_JSON = 0x3;

    int RESULTS_MSG_PACK = 0x4;

    int RESULTS_WITH_PAYLOAD_TYPES = 0x10;

    int RESULTS_WITH_ITEM_ID = 0x20;

    int RESULTS_WITH_RANK = 0x40;

    int RESULTS_WITH_NS_ID = 0x80;

    int RESULTS_WITH_JOINED = 0x100;

    int RESULTS_WITH_RAW = 0x200;

    int RESULTS_NEED_OUTPUT_RANK = 0x400;

    /**
     * Open or create a new namespace and indexes based on passed definition.
     *
     * @param namespace a namespace definition to open
     */
    void openNamespace(NamespaceDefinition namespace);

    /**
     * Add an index based on passed definition.
     *
     * @param namespace the namespace name
     * @param index     an index definition to add
     */
    void addIndex(String namespace, IndexDefinition index);

    /**
     * Modifies namespace item data.
     *
     * @param namespaceName name of a namespace item belongs to
     * @param format        item encoding format (CJSON, JSON)
     * @param data          item data
     * @param mode          modify mode (UPDATE, INSERT, UPSERT, DELETE)
     * @param precepts      precepts (i.e. "id=serial()", "updated_at=now()")
     * @param stateToken    state token
     */
    void modifyItem(String namespaceName, int format, byte[] data, int mode, String[] precepts, int stateToken);

    /**
     * Drop a namespace by name.
     *
     * @param namespaceName a namespace name to drop
     */
    void dropNamespace(String namespaceName);

    /**
     * Close a namespace by name.
     *
     * @param namespaceName a namespace name to close
     */
    void closeNamespace(String namespaceName);

    /**
     * Invoke select query.
     *
     * @param queryData  encoded query data (selected indexes, predicates, etc)
     * @param asJson     format of encoded query data. If asJson = true - JSON format is used, CJSON otherwise.
     * @param fetchCount items count to fetch within a query request
     * @return the request context
     */
    RequestContext selectQuery(byte[] queryData, boolean asJson, int fetchCount);

    /**
     * Invoke delete query.
     *
     * @param queryData encoded query data (selected indexes, predicates, etc)
     */
    void deleteQuery(byte[] queryData);

    /**
     * Invoke update query.
     *
     * @param queryData encoded query data (selected indexes, predicates, etc)
     */
    void updateQuery(byte[] queryData);

    /**
     * Starts a transaction for the given namespace name.
     *
     * @param namespaceName the namespace name
     * @return the transaction context
     */
    TransactionContext beginTx(String namespaceName);

    /**
     * Closes binding to Reindexer instance.
     */
    void close();

}
