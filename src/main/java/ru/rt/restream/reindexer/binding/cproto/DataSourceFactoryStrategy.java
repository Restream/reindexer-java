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

import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.ReindexerConfiguration;
import ru.rt.restream.reindexer.binding.definition.Nodes;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A {@link DataSourceFactory} strategies.
 */
public enum DataSourceFactoryStrategy implements DataSourceFactory {

    /**
     * Next strategy. Selects the next URL cyclically.
     */
    NEXT {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<String> urls = configuration.getUrls();
            configuration.setActive((configuration.getActive() + 1) % urls.size());
            return new PhysicalDataSource(urls.get(configuration.getActive()));
        }
    },

    /**
     * Random strategy. Select the random URL.
     */
    RANDOM {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<String> urls = configuration.getUrls();
            configuration.setActive(ThreadLocalRandom.current().nextInt(urls.size()));
            return new PhysicalDataSource(urls.get(configuration.getActive()));
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
     * PreferWrite strategy. Select the URL of leader node.
     */
    PREFER_WRITE {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<String> urls = getOnlineClusterNodes(configuration).stream()
                    .filter(Nodes.Node::isLeader)
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
                    .filter(Nodes.Node::isFollower)
                    .map(Nodes.Node::getDsn)
                    .collect(Collectors.toList());
            if (urls.isEmpty()) {
                urls = clusterNodes.stream()
                        .filter(Nodes.Node::isLeader)
                        .map(Nodes.Node::getDsn)
                        .collect(Collectors.toList());
            }
            return RANDOM.getDataSource(configuration.toBuilder().urls(urls).build());
        }
    },

    /**
     * PreferRead strategy. It selects the random URL from list of synchronized follower nodes.
     * If the list is empty, it selects the leader node.
     */
    PREFER_READ {
        @Override
        public DataSource getDataSource(DataSourceConfiguration configuration) {
            List<Nodes.Node> clusterNodes = getOnlineClusterNodes(configuration);
            List<String> urls = clusterNodes.stream()
                    .filter(Nodes.Node::isSynchronized)
                    .filter(Nodes.Node::isFollower)
                    .map(Nodes.Node::getDsn)
                    .collect(Collectors.toList());
            if (urls.isEmpty()) {
                urls = clusterNodes.stream()
                        .filter(Nodes.Node::isLeader)
                        .map(Nodes.Node::getDsn)
                        .collect(Collectors.toList());
            }
            return RANDOM.getDataSource(configuration.toBuilder().urls(urls).build());
        }
    };

    /**
     * Get a list of online {@link Nodes.Node} of Reindexer cluster.
     *
     * @param dataSourceConfig need to configure an obtaining of list of online nodes
     * @return a list of online {@link Nodes.Node} of Reindexer cluster
     */
    private static List<Nodes.Node> getOnlineClusterNodes(DataSourceConfiguration dataSourceConfig) {
        List<String> urls = dataSourceConfig.getUrls();
        ReindexerConfiguration configuration = ReindexerConfiguration.builder()
                .dataSourceFactory(RANDOM)
                .urls(urls)
                .connectionPoolSize(1)
                .requestTimeout(Duration.ofSeconds(5));
        try (Reindexer db = configuration.getReindexer()) {
            Namespace<Nodes> ns = db.openNamespace("#replicationstats", NamespaceOptions.defaultOptions(), Nodes.class);
            Query<Nodes> query = ns.query()
                    .select("nodes")
                    .where("type", Query.Condition.EQ, "cluster");
            List<Nodes.Node> nodes = query.findOne()
                    .orElseThrow(() -> new ReindexerException("Cannot to get list of urls from #replicationstats"))
                    .getNodes();
            Predicate<Nodes.Node> allowUnlistedPredicate = dataSourceConfig.isAllowUnlistedDataSource() ? node -> true
                    : node -> urls.contains(node.getDsn());
            return nodes.stream()
                    .filter(Nodes.Node::isOnline)
                    .filter(allowUnlistedPredicate)
                    .collect(Collectors.toList());
        }
    }

}
