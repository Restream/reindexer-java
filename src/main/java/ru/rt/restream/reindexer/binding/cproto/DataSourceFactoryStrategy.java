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

import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.ReindexerConfiguration;
import ru.rt.restream.reindexer.binding.definition.Nodes;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * A {@link DataSourceFactory} strategies.
 */
public enum DataSourceFactoryStrategy implements DataSourceFactory {

    /**
     * Next strategy. Selects the next URL cyclically.
     */
    NEXT {
        private int position;

        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<URI> uris = configuration.getUris();
            URI uri = uris.get(position++);
            if (position == uris.size()) {
                position = 0;
            }
            return new PhysicalDataSource(uri);
        }
    },

    /**
     * Random strategy. Select the random URL.
     */
    RANDOM {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<URI> uris = configuration.getUris();
            int index = ThreadLocalRandom.current().nextInt(uris.size());
            return new PhysicalDataSource(uris.get(index));
        }
    },

    /**
     * Synchronized strategy. Select the random URL from list of synchronized nodes.
     */
    SYNCHRONIZED {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<String> urls = getOnlineClusterNodes(configuration).stream()
                    .filter(Nodes.Node::isSynchronized)
                    .map(Nodes.Node::getDsn)
                    .collect(Collectors.toList());
            return RANDOM.getDataSource(configuration.toBuilder().urls(urls).build());
        }

    },

    /**
     * WriteReq strategy. Select the URL of leader node.
     */
    WRITE_REQ {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<String> urls = getOnlineClusterNodes(configuration).stream()
                    .filter(node -> "leader".equals(node.getRole()))
                    .map(Nodes.Node::getDsn)
                    .collect(Collectors.toList());
            return NEXT.getDataSource(configuration.toBuilder().urls(urls).build());
        }
    },

    /**
     * ReadOnly strategy. It selects the random URL from list of follower nodes.
     * If the list is empty, it selects the leader node.
     */
    READ_ONLY {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<Nodes.Node> clusterNodes = getOnlineClusterNodes(configuration);
            List<String> urls = clusterNodes.stream()
                    .filter(node -> "follower".equals(node.getRole()))
                    .map(Nodes.Node::getDsn)
                    .collect(Collectors.toList());
            if (urls.isEmpty()) {
                urls = clusterNodes.stream()
                        .filter(node -> "leader".equals(node.getRole()))
                        .map(Nodes.Node::getDsn)
                        .collect(Collectors.toList());
            }
            return RANDOM.getDataSource(configuration.toBuilder().urls(urls).build());
        }
    },

    /**
     * PrefferRead strategy. It selects the random URL from list of synchronized follower nodes.
     * If the list is empty, it selects the leader node.
     */
    PREFFER_READ {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<Nodes.Node> clusterNodes = getOnlineClusterNodes(configuration);
            List<String> urls = clusterNodes.stream()
                    .filter(Nodes.Node::isSynchronized)
                    .filter(node -> "follower".equals(node.getRole()))
                    .map(Nodes.Node::getDsn)
                    .collect(Collectors.toList());
            if (urls.isEmpty()) {
                urls = clusterNodes.stream()
                        .filter(node -> "leader".equals(node.getRole()))
                        .map(Nodes.Node::getDsn)
                        .collect(Collectors.toList());
            }
            return RANDOM.getDataSource(configuration.toBuilder().urls(urls).build());
        }
    };

    /**
     * Get a list of online {@link Nodes.Node} of Reindexer claster.
     *
     * @param dataSourceConfig need to configure an obtaining of list of online nodes
     * @return a list of online {@link Nodes.Node} of Reindexer claster
     */
    private static List<Nodes.Node> getOnlineClusterNodes(DataSourceConfiguration dataSourceConfig) {
        Reindexer db = null;
        List<Nodes.Node> nodes;
        List<String> urls = dataSourceConfig.getUrls();
        try {
            db = ReindexerConfiguration.builder()
                    .dataSourceFactory(RANDOM)
                    .urls(urls)
                    .connectionPoolSize(1)
                    .requestTimeout(Duration.ofSeconds(5))
                    .getReindexer();

            db.openNamespace("#replicationstats", NamespaceOptions.defaultOptions(), Nodes.class);

            nodes = db.query("#replicationstats", Nodes.class)
                    .select("nodes")
                    .where("type", Query.Condition.EQ, "cluster")
                    .findOne()
                    .orElseThrow(() -> new ReindexerException("Cannot to get list of urls from #replicationstats"))
                    .getNodes();

            nodes.removeIf(node -> !"online".equals(node.getStatus()));
            if (!dataSourceConfig.isAllowUnlistedDataSource()) {
                nodes.removeIf(node -> !urls.contains(node.getDsn()));
            }
        } finally {
            if (db != null) {
                db.close();
            }
        }
        return nodes;
    }

}
