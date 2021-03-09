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
import ru.rt.restream.reindexer.binding.Consts;
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

/**
 * A {@link Binding} to Reindexer, which establish a connection to Reindexer instance via native calls.
 */
public class Builtin implements Binding {

    private static final Logger LOGGER = LoggerFactory.getLogger(Builtin.class);

    private static final String REINDEXER_VERSION = "v2.14.1";

    private final AtomicLong next = new AtomicLong(0L);

    private final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    private final BuiltinAdapter adapter;

    private final long rx;

    private final Duration timeout;

    /**
     * Creates an instance.
     *
     * @param url            the Reindexer URL
     * @param requestTimeout the request timeout
     */
    public Builtin(String url, Duration requestTimeout) {
        adapter = new BuiltinAdapter();
        timeout = requestTimeout;
        rx = adapter.init();
        URI uri = URI.create(url);
        String path = uri.getPath();
        try {
            ReindexerResponse response = adapter.connect(rx, path, REINDEXER_VERSION);
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
    public void modifyItem(String namespaceName, byte[] data, int mode, String[] precepts, int stateToken) {
        ByteBuffer args = new ByteBuffer()
                .putVString(namespaceName)
                .putVarUInt32(Consts.FORMAT_C_JSON)
                .putVarUInt32(mode)
                .putVarUInt32(stateToken);
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
    public RequestContext selectQuery(byte[] queryData, int fetchCount, long[] ptVersions) {
        ReindexerResponse response = adapter.selectQuery(rx, next.getAndIncrement(), timeout.toMillis(), queryData,
                ptVersions, false);
        checkResponse(response);
        return new BuiltinRequestContext(response);
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        ReindexerResponse response = adapter.deleteQuery(rx, next.getAndIncrement(), timeout.toMillis(), queryData);
        checkResponse(response);
    }

    @Override
    public void updateQuery(byte[] queryData) {
        ReindexerResponse response = adapter.updateQuery(rx, next.getAndIncrement(), timeout.toMillis(), queryData);
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
        return new BuiltinTransactionContext(adapter, rx, txId, next::getAndIncrement, timeout);
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
    public void close() {
        adapter.destroy(rx);
    }

}
