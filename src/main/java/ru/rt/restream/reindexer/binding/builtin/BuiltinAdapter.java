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

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * A Builtin adapter to Reindexer native functions.
 */
public class BuiltinAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BuiltinAdapter.class);

    static {
        try {
            if (SystemUtils.IS_OS_MAC) {
                loadLibrary("libbuiltin-adapter.dylib");
            } else if (SystemUtils.IS_OS_LINUX) {
                loadLibrary("libbuiltin-adapter.so");
            } else {
                throw new UnimplementedException("OS '" + SystemUtils.OS_NAME + "' is not supported");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void loadLibrary(String fileName) throws IOException {
        Path tempFile = Files.createTempFile("rx-connector-", fileName);
        LOGGER.debug("rx: loading {}", tempFile);
        try (InputStream is = BuiltinAdapter.class.getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                throw new IllegalArgumentException("File: '" + fileName + "' is not found");
            }
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
            System.load(tempFile.toAbsolutePath().toString());
            LOGGER.debug("rx: loaded {}", tempFile);
        } finally {
            Files.delete(tempFile);
            LOGGER.debug("rx: deleted {}", tempFile);
        }
    }

    /**
     * Initializes Reindexer instance.
     *
     * @return the Reindexer instance pointer
     */
    public native long init();

    /**
     * Destroys Reindexer instance.
     *
     * @param rx the Reindexer instance pointer
     */
    public native void destroy(long rx);

    /**
     * Initializes Reindexer server.
     *
     * @return the Reindexer server pointer
     */
    public native long initServer();

    /**
     * Destroys Reindexer server.
     *
     * @param svc the Reindexer server pointer
     */
    public native void destroyServer(long svc);

    /**
     * Starts Reindexer server.
     *
     * @param svc        the Reindexer server pointer
     * @param yamlConfig the Reindexer config yaml
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse startServer(long svc, String yamlConfig);

    /**
     * Stops Reindexer server.
     *
     * @param svc the Reindexer server pointer
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse stopServer(long svc);

    /**
     * Returns true if Reindexer server is ready.
     *
     * @param svc the Reindexer server pointer
     * @return true if Reindexer server is ready
     */
    public native boolean isServerReady(long svc);

    /**
     * Returns the Reindexer instance pointer.
     *
     * @param svc      the Reindexer server pointer
     * @param database the Reindexer's database name
     * @param user     the Reindexer's user
     * @param password the Reindexer's password
     * @return the Reindexer instance pointer
     */
    public native long getInstance(long svc, String database, String user, String password);

    /**
     * Connects to Reindexer instance.
     *
     * @param rx      the Reindexer instance pointer
     * @param path    the Reindexer's database path
     * @param version the Reindexer's version
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse connect(long rx, String path, String version);

    /**
     * Opens a namespace.
     *
     * @param rx                    the Reindexer instance pointer
     * @param ctxId                 the context id
     * @param timeout               the execution timeout
     * @param namespaceName         the namespace name
     * @param enabled               true if the storage is enabled
     * @param dropOnFileFormatError true if drop the storage on file format error
     * @param createIfMissing       true if create storage if missing
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse openNamespace(long rx, long ctxId, long timeout, String namespaceName,
                                                  boolean enabled, boolean dropOnFileFormatError, boolean createIfMissing);

    /**
     * Closes a namespace.
     *
     * @param rx            the Reindexer instance pointer
     * @param ctxId         the context id
     * @param timeout       the execution timeout
     * @param namespaceName the namespace name
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse closeNamespace(long rx, long ctxId, long timeout, String namespaceName);

    /**
     * Drops a namespace.
     *
     * @param rx        the Reindexer instance pointer
     * @param ctxId     the context id
     * @param timeout   the execution timeout
     * @param namespace the namespace name
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse dropNamespace(long rx, long ctxId, long timeout, String namespace);

    /**
     * Adds an index.
     *
     * @param rx            the Reindexer instance pointer
     * @param ctxId         the context id
     * @param timeout       the execution timeout
     * @param namespaceName the namespace name
     * @param indexJson     the index JSON string
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse addIndex(long rx, long ctxId, long timeout, String namespaceName, String indexJson);

    /**
     * Modifies an item.
     *
     * @param rx      the Reindexer instance pointer
     * @param ctxId   the context id
     * @param timeout the execution timeout
     * @param args    the execution args (i.e. namespace name, format, mode, state token)
     * @param data    the execution payload
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse modifyItem(long rx, long ctxId, long timeout, byte[] args, byte[] data);

    /**
     * Modifies an item.
     *
     * @param rx   the Reindexer instance pointer
     * @param txId the transaction id
     * @param args the execution args (i.e. format, mode, state token)
     * @param data the execution payload
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse modifyItemTx(long rx, long txId, byte[] args, byte[] data);

    /**
     * Starts a transaction.
     *
     * @param rx            the Reindexer instance pointer
     * @param namespaceName the namespace name
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse beginTx(long rx, String namespaceName);

    /**
     * Commits the transaction.
     *
     * @param rx      the Reindexer instance pointer
     * @param txId    the transaction id
     * @param ctxId   the context id
     * @param timeout the execution timeout
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse commitTx(long rx, long txId, long ctxId, long timeout);

    /**
     * Rollbacks the transaction.
     *
     * @param rx   the Reindexer instance pointer
     * @param txId the transaction id
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse rollbackTx(long rx, long txId);

    /**
     * Executes select query.
     *
     * @param rx       the Reindexer instance pointer
     * @param ctxId    the context id
     * @param timeout  the execution timeout
     * @param data     the query payload (i.e. predicates, joins etc)
     * @param versions the versions
     * @param asJson   true if response should be serialized in JSON format, defaults to CJSON
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse selectQuery(long rx, long ctxId, long timeout, byte[] data, long[] versions, boolean asJson);

    /**
     * Executes delete query.
     *
     * @param rx      the Reindexer instance pointer
     * @param ctxId   the context id
     * @param timeout the execution timeout
     * @param data    the query payload (i.e. predicates, joins etc)
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse deleteQuery(long rx, long ctxId, long timeout, byte[] data);

    /**
     * Executes delete query in the transaction.
     *
     * @param rx   the Reindexer instance pointer
     * @param txId the transaction id
     * @param data the query payload (i.e. predicates, joins etc)
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse deleteQueryTx(long rx, long txId, byte[] data);

    /**
     * Executes update query.
     *
     * @param rx      the Reindexer instance pointer
     * @param ctxId   the context id
     * @param timeout the execution timeout
     * @param data    the query payload (i.e. predicates, joins etc)
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse updateQuery(long rx, long ctxId, long timeout, byte[] data);

    /**
     * Executes update query in the transaction.
     *
     * @param rx   the Reindexer instance pointer
     * @param txId the transaction id
     * @param data the query payload (i.e. predicates, joins etc)
     * @return the {@link ReindexerResponse} to use
     */
    public native ReindexerResponse updateQueryTx(long rx, long txId, byte[] data);

    /**
     * Associates the specified value with the specified key in reindexer namespace.
     *
     * @param rx        the Reindexer instance pointer
     * @param ctxId     the context id
     * @param timeout   the execution timeout
     * @param namespace the namespace name
     * @param key       key with which the specified value is to be associated
     * @param data      value to be associated with the specified key
     */
    public native void putMeta(long rx, long ctxId, long timeout, String namespace, String key, String data);

    /**
     * Returns the value to which the specified key is mapped, or empty string if namespace contains no mapping for the
     * key.
     *
     * @param rx        the Reindexer instance pointer
     * @param ctxId     the context id
     * @param timeout   the execution timeout
     * @param namespace the namespace name
     * @param key       the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or empty string if namespace contains no mapping for the
     * key
     */
    public native ReindexerResponse getMeta(long rx, long ctxId, long timeout, String namespace, String key);
}
