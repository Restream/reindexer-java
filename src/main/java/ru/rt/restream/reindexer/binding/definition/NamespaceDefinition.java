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

import ru.rt.restream.reindexer.ReindexerNamespace;

/**
 * Data-transfer object class, which is used to create namespace.
 */
public class NamespaceDefinition {

    private final String name;

    private final StorageOptions storage;

    /**
     * Constructs the new namespace definition with its name and storage options.
     *
     * @param name    the namespace name
     * @param storage the storage options
     */
    public NamespaceDefinition(String name, StorageOptions storage) {
        this.name = name;
        this.storage = storage;
    }

    /**
     * Construct the new namespace definition from a {@link ReindexerNamespace} object.
     *
     * @param namespace the object by which the new namespace definition is constructed
     * @return {@link NamespaceDefinition}, constructed from the {@link ReindexerNamespace}
     */
    public static NamespaceDefinition fromNamespace(ReindexerNamespace<?> namespace) {
        StorageOptions storageOptions = new StorageOptions(namespace.isEnableStorage(),
                namespace.isDropStorageOnFileFormatError(), namespace.isCreateStorageIfMissing());
        return new NamespaceDefinition(namespace.getName(), storageOptions);
    }

    /**
     * Get the current namespace name.
     *
     * @return the current namespace name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the current namespace storage options.
     *
     * @return the current namespace storage options
     */
    public StorageOptions getStorage() {
        return storage;
    }

    /**
     * Describes namespace storage options.
     */
    public static class StorageOptions {

        private final boolean enabled;

        private final boolean dropOnFileFormatError;

        private final boolean createIfMissing;

        /**
         * Creates new instance.
         *
         * @param enabled               an indication that the namespace storage is enabled
         * @param dropOnFileFormatError drop storage on file format error
         * @param createIfMissing       create new storage if it is not exists
         */
        public StorageOptions(boolean enabled, boolean dropOnFileFormatError, boolean createIfMissing) {
            this.enabled = enabled;
            this.dropOnFileFormatError = dropOnFileFormatError;
            this.createIfMissing = createIfMissing;
        }

        /**
         * Get the indication, that the namespace storage is enabled.
         *
         * @return true, if external storage is enabled
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Get the indication, that the namespace storage will be dropped on file format error.
         *
         * @return true, if namespace external storage will be dropped on file format error
         */
        public boolean isDropOnFileFormatError() {
            return dropOnFileFormatError;
        }

        /**
         * Get the indication, that the namespace storage will be created if not exists.
         *
         * @return true, if namespace storage will be created if not exists
         */
        public boolean isCreateIfMissing() {
            return createIfMissing;
        }
    }

}
