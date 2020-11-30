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

    int START_TRANSACTION = 28;

    int ADD_TX_ITEM = 29;

    int COMMIT_TX = 30;

    int ROLLBACK_TX = 31;

    int COMMIT = 32;

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
     * @param index an index definition to add
     */
    void addIndex(String namespace, IndexDefinition index);

    /**
     * Modifies namespace item data.
     *
     * @param namespaceName name of a namespace item belongs to
     * @param format item encoding format (CJSON, JSON)
     * @param data item data
     * @param mode modify mode (UPDATE, INSERT, UPSERT, DELETE)
     * @param percepts
     * @param stateToken
     */
    void modifyItem(String namespaceName, int format, byte[] data, int mode, String[] percepts, int stateToken);

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
     */
    QueryResult selectQuery(byte[] queryData, boolean asJson, int fetchCount);

    /**
     * Invoke delete query.
     *
     * @param queryData  encoded query data (selected indexes, predicates, etc)
     */
    long deleteQuery(byte[] queryData);

    /**
     * Invoke update query.
     *
     * @param queryData encoded query data (selected indexes, predicates, etc)
     */
    long updateQuery(byte[] queryData);

    /**
     * Fetch query result by requestId.
     *
     * @param requestId query request id
     * @param asJson format of encoded query data. If asJson = true - JSON format is used, CJSON otherwise.
     * @param offset query result offset
     * @param limit items count to fetch within a query request
     * */
    QueryResult fetchResults(long requestId, boolean asJson, int offset, int limit);

    /**
     * Closes query results by requestId.
     *
     * @param requestId query request id
     */
    void closeResults(long requestId);

    /**
     * Closes binding to Reindexer instance.
     */
    void close();

}
