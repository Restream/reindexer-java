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

package ru.rt.restream.reindexer.binding.builtin;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.exceptions.ReindexerExceptionFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static ru.rt.restream.reindexer.binding.Consts.QUERY_FORMAT_V2;
import static ru.rt.restream.reindexer.binding.Consts.REINDEXER_VERSION;

/**
 * A {@link Binding} to Reindexer, which establish a connection to Reindexer instance via native calls.
 */
public class Builtin implements Binding {

    private static final Logger LOGGER = LoggerFactory.getLogger(Builtin.class);

    private static final int NESTED_JOIN_QUERIES_MIN_MAJOR = 5;

    private static final int NESTED_JOIN_QUERIES_MIN_MINOR = 16;

    private static final int NESTED_JOIN_QUERIES_MIN_PATCH = 0;

    private final AtomicLong next = new AtomicLong(0L);

    private final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    private final BuiltinAdapter adapter;

    private final long rx;

    private final Duration timeout;

    private final boolean supportsNestedJoinQueries;

    private final int queryFormatVersion;

    /**
     * Creates an instance.
     *
     * @param uri            the Reindexer URL
     * @param requestTimeout the request timeout
     */
    public Builtin(URI uri, Duration requestTimeout) {
        adapter = new BuiltinAdapter();
        timeout = requestTimeout;
        rx = adapter.init();
        queryFormatVersion = QUERY_FORMAT_V2;
        supportsNestedJoinQueries = isNestedJoinQueriesSupported(adapter.version());
        String path = uri.getPath();
        try {
            ReindexerResponse response = adapter.connect(rx, path, REINDEXER_VERSION, queryFormatVersion);
            checkResponse(response);
        } catch (Exception e) {
            LOGGER.error("rx: connect error", e);
            close();
            throw e;
        }
    }

    /**
     * Creates an instance.
     *
     * @param adapter the {@link BuiltinAdapter} to use
     * @param rx      the Reindexer pointer
     * @param timeout the execution timeout
     */
    public Builtin(BuiltinAdapter adapter, long rx, Duration timeout) {
        this.adapter = adapter;
        this.rx = rx;
        this.timeout = timeout;
        queryFormatVersion = QUERY_FORMAT_V2;
        supportsNestedJoinQueries = isNestedJoinQueriesSupported(adapter.version());
    }

    @Override
    public void openNamespace(NamespaceDefinition namespace) {
        NamespaceDefinition.StorageOptions options = namespace.getStorage();
        ReindexerResponse response = adapter.openNamespace(rx, next.getAndIncrement(), timeout.toMillis(),
                namespace.getName(), options.isEnabled(), options.isDropOnFileFormatError(), options.isCreateIfMissing());
        checkResponse(response);
    }

    @Override
    public void addIndex(String namespace, IndexDefinition index) {
        ReindexerResponse response = adapter.addIndex(rx, next.getAndIncrement(), timeout.toMillis(), namespace,
                gson.toJson(index));
        checkResponse(response);
    }

    @Override
    public void updateIndex(String namespace, IndexDefinition index) {
        ReindexerResponse response = adapter.updateIndex(rx, next.getAndIncrement(), timeout.toMillis(), namespace,
                gson.toJson(index));
        checkResponse(response);
    }

    @Override
    public void dropIndex(String namespace, String indexName) {
        ReindexerResponse response = adapter.dropIndex(rx, next.getAndIncrement(), timeout.toMillis(), namespace,
                indexName);
        checkResponse(response);
    }

    @Override
    public void modifyItem(String namespaceName, byte[] data, int format, int mode, String[] precepts, int stateToken) {
        ByteBuffer args = new ByteBuffer()
                .putVString(namespaceName)
                .putVarUInt32(format)
                .putVarUInt32(mode)
                .putVarInt32(stateToken);
        args.putVarUInt32(precepts.length);
        for (String precept : precepts) {
            args.putVString(precept);
        }
        ReindexerResponse response = adapter.modifyItem(rx, next.getAndIncrement(), timeout.toMillis(), args.bytes(), data);
        checkResponse(response);
    }

    @Override
    public void dropNamespace(String namespaceName) {
        ReindexerResponse response = adapter.dropNamespace(rx, next.getAndIncrement(), timeout.toMillis(), namespaceName);
        checkResponse(response);
    }

    @Override
    public void closeNamespace(String namespaceName) {
        ReindexerResponse response = adapter.closeNamespace(rx, next.getAndIncrement(), timeout.toMillis(), namespaceName);
        checkResponse(response);
    }

    @Override
    public RequestContext select(String query, boolean asJson, int fetchCount, long[] ptVersions) {
        ReindexerResponse response = adapter.select(rx, next.getAndIncrement(), timeout.toMillis(), query, asJson,
                ptVersions);
        checkResponse(response);
        return new BuiltinRequestContext(response, queryFormatVersion);
    }

    @Override
    public RequestContext selectQuery(byte[] queryData, int fetchCount, long[] ptVersions, boolean asJson) {
        ReindexerResponse response = adapter.selectQuery(rx, next.getAndIncrement(), timeout.toMillis(), queryData,
                ptVersions, asJson);
        checkResponse(response);
        return new BuiltinRequestContext(response, queryFormatVersion);
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        ReindexerResponse response = adapter.deleteQuery(rx, next.getAndIncrement(), timeout.toMillis(), queryData);
        checkResponse(response);
    }

    @Override
    public void updateQuery(byte[] queryData, long[] ptVersions) {
        ReindexerResponse response = adapter.updateQuery(rx, next.getAndIncrement(), timeout.toMillis(), queryData, ptVersions);
        checkResponse(response);
    }

    @Override
    public TransactionContext beginTx(String namespaceName) {
        ReindexerResponse response = adapter.beginTx(rx, namespaceName);
        checkResponse(response);
        long txId = -1L;
        Object[] arguments = response.getArguments();
        if (arguments.length > 0) {
            Object arg = arguments[0];
            if (arg instanceof Long) {
                txId = (long) arg;
            }
        }
        return new BuiltinTransactionContext(adapter, rx, txId, next::getAndIncrement, timeout, queryFormatVersion);
    }

    @Override
    public void putMeta(String namespace, String key, String data) {
        adapter.putMeta(rx, next.getAndIncrement(), timeout.toMillis(), namespace, key, data);
    }

    @Override
    public String getMeta(String namespace, String key) {
        ReindexerResponse response = adapter.getMeta(rx, next.getAndIncrement(), timeout.toMillis(), namespace, key);
        checkResponse(response);
        return new String((byte[])response.getArguments()[1], StandardCharsets.UTF_8);
    }

    private void checkResponse(ReindexerResponse response) {
        if (response.hasError()) {
            throw ReindexerExceptionFactory.fromResponse(response);
        }
    }

    @Override
    public int queryFormatVersion() {
        return queryFormatVersion;
    }

    @Override
    public boolean supportsNestedJoinQueries() {
        return supportsNestedJoinQueries;
    }

    private boolean isNestedJoinQueriesSupported(String version) {
        int[] parsedVersion = parseVersion(version);
        if (parsedVersion[0] != NESTED_JOIN_QUERIES_MIN_MAJOR) {
            return parsedVersion[0] > NESTED_JOIN_QUERIES_MIN_MAJOR;
        }
        if (parsedVersion[1] != NESTED_JOIN_QUERIES_MIN_MINOR) {
            return parsedVersion[1] > NESTED_JOIN_QUERIES_MIN_MINOR;
        }
        return parsedVersion[2] >= NESTED_JOIN_QUERIES_MIN_PATCH;
    }

    private int[] parseVersion(String version) {
        String normalized = version.startsWith("v") ? version.substring(1) : version;
        String[] parts = normalized.split("\\D+");
        int[] result = new int[3];
        for (int i = 0; i < result.length && i < parts.length; i++) {
            if (!parts[i].isEmpty()) {
                result[i] = Integer.parseInt(parts[i]);
            }
        }
        return result;
    }

    @Override
    public void close() {
        adapter.destroy(rx);
    }

}
