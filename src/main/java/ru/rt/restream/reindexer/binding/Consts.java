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
package ru.rt.restream.reindexer.binding;

/**
 * Different constants from type_consts.h and reindexer_ctypes.h
 */
public final class Consts {

    public static final String REINDEXER_VERSION = "v5.4.0";
    public static final String DEF_APP_NAME = "java-connector";
    public static final String APP_PROPERTY_NAME = "app.name";

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

    public static final int VALUE_INT_64 = 0;
    public static final int VALUE_DOUBLE = 1;
    public static final int VALUE_STRING = 2;
    public static final int VALUE_BOOL = 3;
    public static final int VALUE_NULL = 4;
    public static final int VALUE_INT = 8;
    public static final int VALUE_UNDEFINED = 9;
    public static final int VALUE_COMPOSITE = 10;
    public static final int VALUE_TUPLE = 11;
    public static final int VALUE_UUID = 12;
    public static final int VALUE_FLOAT_VECTOR = 13;
    public static final int VALUE_FLOAT = 14;

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
    public static final int QUERY_RESULT_SHARDING_VERSION = 3;
    public static final int QUERY_RESULT_SHARD_ID = 4;
    // incarnation tags are not supported int java connector
    public static final int QUERY_RESULT_INCARNATION_TAGS = 5;
    public static final int QUERY_RESULT_RANK_FORMAT = 6;

    public static final int KNN_QUERY_TYPE_BASE = 0;
    public static final int KNN_QUERY_TYPE_BRUTE_FORCE = 1;
    public static final int KNN_QUERY_TYPE_HNSW = 2;
    public static final int KNN_QUERY_TYPE_IVF = 3;

    public static final int KNN_QUERY_PARAMS_VERSION = 1;

    public static final int RESULTS_FORMAT_MASK = 0xF;
    public static final int RESULTS_PURE = 0x0;
    public static final int RESULTS_PTRS = 0x1;
    public static final int RESULTS_C_JSON = 0x2;
    public static final int RESULTS_JSON = 0x3;

    public static final int RESULTS_WITH_PAYLOAD_TYPES = 0x10;
    public static final int RESULTS_WITH_ITEM_ID = 0x20;
    public static final int RESULTS_WITH_RANK = 0x40;
    public static final int RESULTS_WITH_NS_ID = 0x80;
    public static final int RESULTS_WITH_JOINED = 0x100;
    public static final int RESULTS_WITH_SHARD_ID = 0x800;
    public static final int RESULTS_SUPPORT_IDLE_TIMEOUT = 0x2000;

    public static final int INDEX_OPT_PK = 1 << 7;
    public static final int INDEX_OPT_ARRAY = 1 << 6;
    public static final int INDEX_OPT_DENSE = 1 << 5;
    public static final int INDEX_OPT_APPENDABLE = 1 << 4;
    public static final int INDEX_OPT_SPARSE = 1 << 3;

    public static final int STORAGE_OPT_ENABLED = 1;
    public static final int STORAGE_OPT_DROP_ON_FILE_FORMAT_ERROR = 1 << 1;
    public static final int STORAGE_OPT_CREATE_IF_MISSING = 1 << 2;

    public static final int CONNECT_OPT_OPEN_NAMESPACES = 1;
    public static final int CONNECT_OPT_ALLOW_NAMESPACE_ERRORS = 1 << 1;
    public static final int CONNECT_OPT_AUTOREPAIR = 1 << 2;
    public static final int CONNECT_OPT_WARN_VERSION = 1 << 4;

    public static final long BINDING_CAPABILITY_QR_IDLE_TIMEOUTS = 1;
    public static final long BINDING_CAPABILITY_RESULTS_WITH_SHARD_IDS = 1 << 1;
    // incarnation tags are not supported int java connector
    public static final long BINDING_CAPABILITY_NAMESPACE_INCARNATIONS = 1 << 2;
    public static final long BINDING_CAPABILITY_COMPLEX_RANK = 1 << 3;

    public static final int RANK_FORMAT_SINGLE_FLOAT = 0;
    public static final float EMPTY_RANK = Float.NEGATIVE_INFINITY;

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

    public static final int SHARDING_NOT_SET = -1;
    public static final int SHARDING_PROXY_OFF = -2;
    public static final int NOT_SHARDED = -3;

}
