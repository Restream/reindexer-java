package ru.rt.restream.reindexer.connector.binding;

import ru.rt.restream.reindexer.connector.binding.def.IndexDef;

public interface Binding {

    short PING = 0;
    short LOGIN = 1;
    short OPEN_DATABASE = 2;
    short CLOSE_DATABASE = 3;
    short DROP_DATABASE = 4;
    short OPEN_NAMESPACE = 16;
    short CLOSE_NAMESPACE = 17;
    short DROP_NAMESPACE = 18;
    short ADD_INDEX = 21;
    short ENUM_NAMESPACES = 22;
    short DROP_INDEX = 24;
    short UPDATE_INDEX = 25;
    short START_TRANSACTION = 28;
    short ADD_TX_ITEM = 29;
    short COMMIT_TX = 30;
    short ROLLBACK_TX = 31;
    short COMMIT = 32;
    short MODIFY_ITEM = 33;
    short DELETE_QUERY = 34;
    short SELECT = 48;
    short SELECT_SQL = 49;
    short FETCH_RESULTS = 50;
    short CLOSE_RESULTS = 51;
    short GET_META = 64;
    short PUT_META = 65;
    short ENUM_META = 66;
    short CODE_MAX = 128;



    void openNamespace(String namespace, boolean enableStorage, boolean dropOnFileFormatError);

    void addIndex(String namespace, IndexDef index);

    void modifyItem(int nsHash, String namespace, int format, byte[] data, int mode, String[] percepts, int stateToken);

    void dropNamespace(String namespace);

    void closeNamespace(String namespace);

    QueryResult selectQuery(byte[] queryData, boolean asJson, int fetchCount);
}
