package ru.rt.restream.reindexer.connector.binding;

public final class Command {

    public static final short PING = 0;

    public static final short LOGIN = 1;

    public static final short OPEN_DATABASE = 2;

    public static final short CLOSE_DATABASE = 3;

    public static final short DROP_DATABASE = 4;

    public static final short OPEN_NAMESPACE = 16;

    public static final short CLOSE_NAMESPACE = 17;

    public static final short DROP_NAMESPACE = 18;

    public static final short ADD_INDEX = 21;

    public static final short ENUM_NAMESPACES = 22;

    public static final short DROP_INDEX = 24;

    public static final short UPDATE_INDEX = 25;

    public static final short START_TRANSACTION = 28;

    public static final short ADD_TX_ITEM = 29;

    public static final short COMMIT_TX = 30;

    public static final short ROLLBACK_TX = 31;

    public static final short COMMIT = 32;

    public static final short MODIFY_ITEM = 33;

    public static final short DELETE_QUERY = 34;

    public static final short SELECT = 48;

    public static final short SELECT_SQL = 49;

    public static final short FETCH_RESULTS = 50;

    public static final short CLOSE_RESULTS = 51;

    public static final short GET_META = 64;

    public static final short PUT_META = 65;

    public static final short ENUM_META = 66;

    public static final short CODE_MAX = 128;

}
