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

    int SELECT = 48;

    int SELECT_SQL = 49;

    int FETCH_RESULTS = 50;

    int CLOSE_RESULTS = 51;

    int GET_META = 64;

    int PUT_META = 65;

    int ENUM_META = 66;

    int CODE_MAX = 128;

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
     * @param namespaceHash must be same to the same namespaces
     * @param namespaceName name of a namespace item belongs to
     * @param format item encoding format (CJSON, JSON)
     * @param data item data
     * @param mode modify mode (UPDATE, INSERT, UPSERT, DELETE)
     * @param percepts
     * @param stateToken
     */
    void modifyItem(int namespaceHash, String namespaceName, int format, byte[] data, int mode, String[] percepts,
                    int stateToken);

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
     * Invoke select query to database.
     *
     * @param queryData  encoded query data (selected indexes, predicates, etc)
     * @param asJson     format of encoded query data. If asJson = true - JSON format is used, CJSON otherwise.
     * @param fetchCount items count to fetch within a query request
     */
    QueryResult selectQuery(byte[] queryData, boolean asJson, int fetchCount);

    /**
     * Fetch query result by requestId.
     *
     * @param requestId query request id
     * @param asJson format of encoded query data. If asJson = true - JSON format is used, CJSON otherwise.
     * @param offset query result offset
     * @param limit items count to fetch within a query request
     * */
    QueryResult fetchResults(long requestId, boolean asJson, int offset, int limit);
}
