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

package ru.rt.restream.reindexer.binding.cproto;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.net.ssl.SSLSocketFactory;

/**
 * A {@link DataSource} that creates a {@link PhysicalConnection}.
 */
public class PhysicalDataSource implements DataSource {

    private final String host;

    private final int port;

    private final String user;

    private final String password;

    private final String database;

    private final SSLSocketFactory sslSocketFactory;

    /**
     * Creates an instance.
     *
     * @param url the URL to use
     * @deprecated Use {@link #PhysicalDataSource(String, SSLSocketFactory)}
     * to connect to Reindexer using cprotos (SSL/TLS) protocol.
     */
    @Deprecated
    public PhysicalDataSource(String url) {
        this(url, null);
    }

    /**
     * Creates an instance.
     *
     * @param url              the URL to use
     * @param sslSocketFactory the {@link SSLSocketFactory} socket factory to use
     */
    public PhysicalDataSource(String url, SSLSocketFactory sslSocketFactory) {
        URI uri = URI.create(url);
        host = uri.getHost();
        port = uri.getPort();
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] userInfoArray = userInfo.split(":");
            if (userInfoArray.length == 2) {
                user = userInfoArray[0];
                password = userInfoArray[1];
            } else {
                throw new IllegalArgumentException("Invalid username or password in the URL");
            }
        } else {
            user = "";
            password = "";
        }
        database = uri.getPath().substring(1);
        this.sslSocketFactory = sslSocketFactory;
    }

    @Override
    public Connection getConnection(Duration timeout, ScheduledThreadPoolExecutor scheduler) {
        return new PhysicalConnection(host, port, user, password, database, sslSocketFactory, timeout, scheduler);
    }

    @Override
    public String toString() {
        return host + ":" + port + "/" + database;
    }

}
