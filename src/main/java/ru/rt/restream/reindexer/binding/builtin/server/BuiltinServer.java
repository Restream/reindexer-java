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

package ru.rt.restream.reindexer.binding.builtin.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.binding.builtin.Builtin;
import ru.rt.restream.reindexer.binding.builtin.BuiltinAdapter;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.exceptions.ReindexerException;
import ru.rt.restream.reindexer.exceptions.ReindexerExceptionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A {@link Binding} to Reindexer, which establish a connection to Reindexer instance via native calls.
 * The difference from {@link Builtin} binding is that in addition, a reindexer server is launched that can
 * receive client requests using the RPC protocol.
 */
public class BuiltinServer implements Binding {

    private static final Logger LOGGER = LoggerFactory.getLogger(BuiltinServer.class);

    private final BuiltinAdapter adapter;

    private final Binding builtin;

    private final long svc;

    /**
     * Creates an instance.
     *
     * @param url                  the Reindexer URL
     * @param serverConfigFile     the server config file
     * @param serverStartupTimeout the server startup timeout
     * @param requestTimeout       the request timeout
     */
    public BuiltinServer(String url, String serverConfigFile, Duration serverStartupTimeout, Duration requestTimeout) {
        String yamlConfig = readYamlConfig(serverConfigFile);
        adapter = new BuiltinAdapter();
        svc = adapter.initServer();
        Thread serverThread = new Thread(() -> {
            ReindexerResponse response = adapter.startServer(svc, yamlConfig);
            if (response.hasError()) {
                LOGGER.error("rx: startServer error: {}", response.getErrorMessage());
            }
            LOGGER.debug("rx: startServer finished");
        });
        serverThread.start();
        Instant startupDeadline = Instant.now().plus(serverStartupTimeout);
        while (!adapter.isServerReady(svc)) {
            if (Instant.now().isAfter(startupDeadline)) {
                throw new ReindexerException("Server startup timeout");
            }
            try {
                TimeUnit.SECONDS.sleep(1L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ReindexerException("Interrupted while waiting for server to startup");
            }
        }
        URI uri = URI.create(url);
        String user = "";
        String password = "";
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] userInfoArray = userInfo.split(":");
            if (userInfoArray.length == 2) {
                user = userInfoArray[0];
                password = userInfoArray[1];
            } else {
                throw new IllegalArgumentException("Invalid username or password in the URL");
            }
        }
        String database = uri.getHost();
        if (database == null) {
            throw new IllegalArgumentException("Invalid database name in the URL");
        }
        long rx = adapter.getInstance(svc, database, user, password);
        builtin = new Builtin(adapter, rx, requestTimeout);
    }

    private String readYamlConfig(String serverConfigFile) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(serverConfigFile)) {
            if (is == null) {
                throw new IllegalArgumentException("Server config file: '" + serverConfigFile + "' is not found");
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                return br.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void openNamespace(NamespaceDefinition namespace) {
        builtin.openNamespace(namespace);
    }

    @Override
    public void addIndex(String namespace, IndexDefinition index) {
        builtin.addIndex(namespace, index);
    }

    @Override
    public void modifyItem(String namespaceName, byte[] data, int mode, String[] precepts, int stateToken) {
        builtin.modifyItem(namespaceName, data, mode, precepts, stateToken);
    }

    @Override
    public void dropNamespace(String namespaceName) {
        builtin.dropNamespace(namespaceName);
    }

    @Override
    public void closeNamespace(String namespaceName) {
        builtin.closeNamespace(namespaceName);
    }

    @Override
    public RequestContext selectQuery(byte[] queryData, int fetchCount, long[] ptVersions) {
        return builtin.selectQuery(queryData, fetchCount, ptVersions);
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        builtin.deleteQuery(queryData);
    }

    @Override
    public void updateQuery(byte[] queryData) {
        builtin.updateQuery(queryData);
    }

    @Override
    public TransactionContext beginTx(String namespaceName) {
        return builtin.beginTx(namespaceName);
    }

    @Override
    public void close() {
        ReindexerResponse response = adapter.stopServer(svc);
        if (response.hasError()) {
            throw ReindexerExceptionFactory.fromResponse(response);
        }
        adapter.destroyServer(svc);
    }

}
