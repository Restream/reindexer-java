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

package ru.rt.restream.reindexer.db;

import org.apache.commons.io.FileUtils;
import ru.rt.restream.category.CprotoTest;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.ReindexerConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * Creates a Reindexer instances for tests.
 * This class is not thread safe.
 */
public class DbLocator {

    private static final String BUILTIN_DB_PATH = "/tmp/reindex/builtin_test";

    // if change the path, need to synchronized it with path in default-builtin-server-config.yml
    private static final String BUILTINSERVER_DB_PATH = "/tmp/reindex";

    private static final Map<Type, ClearDbReindexer> instancesForUse = new HashMap<>();

    private static final Map<ClearDbReindexer, String> instancesForClose = new HashMap<>();

    /**
     * Environment property, it allows to specify CPROTO DataSource urls for {@link CprotoTest}.
     */
    private static final String CPROTO_DSNS_PROPERTY = "CprotoDsns";

    private static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";

    // A certificate that is valid for 10 years, used to test CPROTOS (SSL/TLS) connection.
    private static final String SSL_CERT_PATH = "builtin-server.crt";

    private static final String SSL_CERT_KEY_PATH = "builtin-server.key";

    private static final String TRUST_STORE_PATH = "builtin-server.jks";

    private static final String TRUST_STORE_PASSWORD = "password";

    // Ensures that the builtin server is started only once.
    private static boolean serverStarted = false;

    public static ClearDbReindexer getDb(Type type) {
        ClearDbReindexer db = instancesForUse.get(type);
        if (db == null) {
            db = addDbInstance(type);
        }
        return db;
    }

    static void closeAllDbInstances() throws IOException {
        for (Map.Entry<ClearDbReindexer, String> entry : instancesForClose.entrySet()) {
            Reindexer db = entry.getKey();
            String path = entry.getValue();
            if (db != null) {
                db.close();
            }
            if (path != null) {
                FileUtils.deleteDirectory(new File(path));
            }
        }
        instancesForClose.clear();
        instancesForUse.clear();
        serverStarted = false;
    }

    private static ClearDbReindexer addDbInstance(Type type) {
        switch (type) {
            case BUILTIN:
                ClearDbReindexer builtinDb = new ClearDbReindexer(ReindexerConfiguration.builder()
                        .url("builtin://" + BUILTIN_DB_PATH)
                        .getReindexer().getBinding());
                instancesForUse.put(Type.BUILTIN, builtinDb);
                instancesForClose.put(builtinDb, BUILTIN_DB_PATH);
                return builtinDb;
            case CPROTOS:
            case CPROTO:
                ReindexerConfiguration cprotoConfig = ReindexerConfiguration.builder()
                        .connectionPoolSize(4)
                        .sslSocketFactory(getSslSocketFactory(type))
                        .requestTimeout(Duration.ofSeconds(30L));

                List<String> urls = getCprotoDbUrlsFromProperty();
                if (urls.isEmpty()) {
                    // run builtinserver, not for own use, only for use in cproto datasource
                    String builtinServerCprotoUrl = runDefaultBuiltinServerDbInstance(type);
                    urls.add(builtinServerCprotoUrl);
                }
                urls.forEach(cprotoConfig::url);

                ClearDbReindexer cprotoDb = new ClearDbReindexer(cprotoConfig.getReindexer().getBinding());
                instancesForUse.put(Type.CPROTO, cprotoDb);
                instancesForClose.put(cprotoDb, null);
                return cprotoDb;
            default:
                throw new IllegalArgumentException("Type of Reindexer DB " + type + " is not supported");
        }
    }

    private static SSLSocketFactory getSslSocketFactory(Type type) {
        // Return null if protocol is not cprotos.
        if (type != Type.CPROTOS) {
            return null;
        }
        // Return null if trust store is specified then default SSLSocketFactory is used.
        String trustStore = System.getProperty(TRUST_STORE_PROPERTY, null);
        if (trustStore != null) {
            return null;
        }
        // Load KeyStore from the classpath and build SSLSocketFactory.
        try (InputStream is = DbLocator.class.getClassLoader().getResourceAsStream(TRUST_STORE_PATH)) {
            if (is == null) {
                throw new IllegalArgumentException(TRUST_STORE_PATH + " not found in classpath");
            }
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(is, TRUST_STORE_PASSWORD.toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext.getSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getCprotoDbUrlsFromProperty() {
        List<String> urls = new ArrayList<>();

        String cprotoUrls = System.getProperty(CPROTO_DSNS_PROPERTY, null);
        if (cprotoUrls != null) {
            Arrays.stream(cprotoUrls.split(","))
                    .filter(s -> !s.isEmpty())
                    .filter(s -> s.startsWith("cproto://") || s.startsWith("cprotos://"))
                    .forEach(urls::add);
        }
        return urls;
    }

    private static String runDefaultBuiltinServerDbInstance(Type type) {
        if (!serverStarted) {
            // Copy the SSL certificate and key to the Reindexer directory.
            copyResourceToReindexerDirectory(SSL_CERT_PATH);
            copyResourceToReindexerDirectory(SSL_CERT_KEY_PATH);
            ClearDbReindexer server = new ClearDbReindexer(ReindexerConfiguration.builder()
                    .url("builtinserver://items")
                    .getReindexer().getBinding());
            instancesForClose.put(server, BUILTINSERVER_DB_PATH);
            serverStarted = true;
        }
        if (type == Type.CPROTOS) {
            return "cprotos://localhost:6535/items";
        }
        return "cproto://localhost:6534/items";
    }

    private static void copyResourceToReindexerDirectory(String fileName) {
        try (InputStream is = DbLocator.class.getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                throw new IllegalArgumentException(fileName + " not found in classpath");
            }
            Path reindexerDir = Paths.get(BUILTINSERVER_DB_PATH);
            Files.createDirectories(reindexerDir);
            Path target = reindexerDir.resolve(fileName);
            Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public enum Type {
        BUILTIN,
        CPROTOS,
        CPROTO
    }

}
