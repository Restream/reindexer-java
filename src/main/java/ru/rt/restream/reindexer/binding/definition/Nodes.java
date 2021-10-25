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

package ru.rt.restream.reindexer.binding.definition;

import ru.rt.restream.reindexer.annotations.Json;

import java.util.List;

/**
 * A list of nodes of reindexer cluster.
 */
public class Nodes {

    private List<Node> nodes;

    /**
     * Get a list of nodes of reindexer cluster.
     */
    public List<Node> getNodes() {
        return nodes;
    }

    /**
     * Set a list of nodes of reindexer cluster.
     */
    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    /**
     * State of node of reindexer cluster.
     */
    public static class Node {

        /**
         * DataSource name - url like "cproto://host:port/database_name".
         */
        @Json("dsn")
        private String dsn;

        /**
         * Node's server ID.
         */
        @Json("server_id")
        private int serverId;

        /**
         * Number of updates, which are awaiting replication to this specific follower.
         */
        @Json("pended_updates_count")
        private int pendedUpdatesCount;

        /**
         * Node's status: none, online, offline or raft_error.
         */
        @Json("status")
        private String status;

        /**
         * Node's role: leader or follower.
         */
        @Json("role")
        private String role;

        /**
         * Synchronization status. Shows if all the approved updates were replicated to this node.
         */
        @Json("is_synchronized")
        private boolean isSynchronized;

        /**
         * Namespaces which are configured for this node.
         */
        @Json("namespaces")
        private List<String> namespaces;

        /**
         * Get datasource name - url like "cproto://host:port/database_name".
         *
         * @return datasource name
         */
        public String getDsn() {
            return dsn;
        }

        /**
         * Set datasource name - url like "cproto://host:port/database_name".
         *
         * @param dsn datasource name
         */
        public void setDsn(String dsn) {
            this.dsn = dsn;
        }

        /**
         * Get node's server ID.
         *
         * @return node's server ID
         */
        public int getServerId() {
            return serverId;
        }

        /**
         * Set node's server ID.
         *
         * @param serverId node's server ID
         */
        public void setServerId(int serverId) {
            this.serverId = serverId;
        }

        /**
         * Get number of updates, which are awaiting replication to this specific follower.
         *
         * @return number of updates
         */
        public int getPendedUpdatesCount() {
            return pendedUpdatesCount;
        }

        /**
         * Set number of updates, which are awaiting replication to this specific follower.
         *
         * @param pendedUpdatesCount number of updates, which are awaiting replication
         */
        public void setPendedUpdatesCount(int pendedUpdatesCount) {
            this.pendedUpdatesCount = pendedUpdatesCount;
        }

        /**
         * Get node's status: none, online, offline or raft_error.
         *
         * @return node's status
         */
        public String getStatus() {
            return status;
        }

        /**
         * Set node's status: none, online, offline or raft_error.
         *
         * @param status node's status: none, online, offline or raft_error
         */
        public void setStatus(String status) {
            this.status = status;
        }

        /**
         * Get node's role: leader or follower.
         *
         * @return node's role
         */
        public String getRole() {
            return role;
        }

        /**
         * Set node's role: leader or follower.
         *
         * @param role node's role: leader or follower
         */
        public void setRole(String role) {
            this.role = role;
        }

        /**
         * Get synchronization status.
         *
         * @return true if all the approved updates were replicated to this node
         */
        public boolean isSynchronized() {
            return isSynchronized;
        }

        /**
         * Set synchronization status.
         *
         * @param isSynchronized true if all the approved updates were replicated to this node
         */
        public void setIsSynchronized(boolean isSynchronized) {
            this.isSynchronized = isSynchronized;
        }

        /**
         * Get namespaces which are configured for this node.
         *
         * @return namespaces which are configured for this node
         */
        public List<String> getNamespaces() {
            return namespaces;
        }

        /**
         * Set namespaces which are configured for this node.
         *
         * @param namespaces namespaces which are configured for this node
         */
        public void setNamespaces(List<String> namespaces) {
            this.namespaces = namespaces;
        }
    }
}
