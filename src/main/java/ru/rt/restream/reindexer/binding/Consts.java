package ru.rt.restream.reindexer.binding;

/**
 * Different constants. TODO: move to classes
 */
public class Consts {

    public static final int ERROR = 1;
    public static final int WARNING = 2;
    public static final int INFO = 3;
    public static final int TRACE = 4;

    public static final int AGG_SUM = 0;
    public static final int AGG_AVG = 1;
    public static final int AGG_FACET = 2;
    public static final int AGG_MIN = 3;
    public static final int AGG_MAX = 4;

    public static final int COLLATE_NONE = 0;
    public static final int COLLATE_ASCII = 1;
    public static final int COLLATE_UTF_8 = 2;
    public static final int COLLATE_NUMERIC = 3;
    public static final int COLLATE_CUSTOM = 4;

    public static final int OP_OR = 1;
    public static final int OP_AND = 2;
    public static final int OP_NOT = 3;

    public static final int VALUE_INT_64 = 0;
    public static final int VALUE_DOUBLE = 1;
    public static final int VALUE_STRING = 2;
    public static final int VALUE_BOOL = 3;
    public static final int VALUE_NULL = 4;
    public static final int VALUE_INT = 8;
    public static final int VALUE_UNDEFINED = 9;
    public static final int VALUE_COMPOSITE = 10;
    public static final int VALUE_TUPLE = 11;

    public static final int QUERY_CONDITION = 0;
    public static final int QUERY_DISTINCT = 1;
    public static final int QUERY_SORT_INDEX = 2;
    public static final int QUERY_JOIN_ON = 3;
    public static final int QUERY_LIMIT = 4;
    public static final int QUERY_OFFSET = 5;
    public static final int QUERY_REQ_TOTAL = 6;
    public static final int QUERY_DEBUG_LEVEL = 7;
    public static final int QUERY_AGGREGATION = 8;
    public static final int QUERY_SELECT_FILTER = 9;
    public static final int QUERY_SELECT_FUNCTION = 10;
    public static final int QUERY_END = 11;
    public static final int QUERY_EXPLAIN = 12;
    public static final int QUERY_EQUAL_POSITION = 13;
    public static final int QUERY_UPDATE_FIELD = 14;
    public static final int QUERY_JOIN_CONDITION = 20;
    public static final int QUERY_DROP_FIELD = 21;
    public static final int QUERY_UPDATE_FIELD_V2 = 25;

    public static final int LEFT_JOIN = 0;
    public static final int INNER_JOIN = 1;
    public static final int OR_INNER_JOIN = 2;
    public static final int MERGE = 3;

    public static final int CACHE_MODE_ON = 0;
    public static final int CACHE_MODE_AGGRESSIVE = 1;
    public static final int CACHE_MODE_OFF = 2;

    public static final int FORMAT_JSON = 0;
    public static final int FORMAT_C_JSON = 1;

    public static final int MODE_NO_CALC = 0;
    public static final int MODE_CACHED_TOTAL = 1;
    public static final int MODE_ACCURATE_TOTAL = 2;

    public static final int QUERY_RESULT_END = 0;
    public static final int QUERY_RESULT_AGGREGATION = 1;
    public static final int QUERY_RESULT_EXPLAIN = 2;

    public static final int RESULTS_FORMAT_MASK = 0xF;
    public static final int RESULTS_PURE = 0x0;
    public static final int RESULTS_PTRS = 0x1;
    public static final int RESULTS_C_JSON = 0x2;
    public static final int RESULTS_JSON = 0x3;

    public static final int RESULTS_WITH_PAYLOAD_TYPES = 0x10;
    public static final int RESULTS_WITH_ITEM_ID = 0x20;
    public static final int RESULTS_WITH_PERCENTS = 0x40;
    public static final int RESULTS_WITH_NS_ID = 0x80;
    public static final int RESULTS_WITH_JOINED = 0x100;

    public static final int INDEX_OPT_PK = 1 << 7;
    public static final int INDEX_OPT_ARRAY = 1 << 6;
    public static final int INDEX_OPT_DENSE = 1 << 5;
    public static final int INDEX_OPT_APPENDABLE = 1 << 4;
    public static final int INDEX_OPT_SPARSE = 1 << 3;

    public static final int STORAGE_OPT_ENABLED = 1;
    public static final int STORAGE_OPT_DROP_ON_FILE_FORMAT_ERROR = 1 << 1;
    public static final int STORAGE_OPT_CREATE_IF_MISSING = 1 << 2;

    public static final int ERR_OK = 0;
    public static final int ERR_PARSE_SQL = 1;
    public static final int ERR_QUERY_EXEC = 2;
    public static final int ERR_PARAMS = 3;
    public static final int ERR_LOGIC = 4;
    public static final int ERR_PARSE_JSON = 5;
    public static final int ERR_PARSE_DSL = 6;
    public static final int ERR_CONFLICT = 7;
    public static final int ERR_PARSE_BIN = 8;
    public static final int ERR_FORBIDDEN = 9;
    public static final int ERR_WAS_RELOCK = 10;
    public static final int ERR_NOT_VALID = 11;
    public static final int ERR_NETWORK = 12;
    public static final int ERR_NOT_FOUND = 13;
    public static final int ERR_STATE_INVALIDATED = 14;

}
