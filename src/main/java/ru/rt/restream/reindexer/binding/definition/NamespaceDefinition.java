package ru.rt.restream.reindexer.binding.definition;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import ru.rt.restream.reindexer.Namespace;

/**
 * Data-transfer object class, which is used to create namespace.
 */
@Getter
@AllArgsConstructor
public class NamespaceDefinition {

    private final String name;

    private final StorageOptions storage;

    public static NamespaceDefinition fromNamespace(Namespace<?> namespace) {
        StorageOptions storageOptions = StorageOptions.builder()
                .enabled(namespace.isEnableStorage())
                .createIfMissing(namespace.isCreateStorageIfMissing())
                .dropOnFileFormatError(namespace.isDropStorageOnFileFormatError())
                .build();
        return new NamespaceDefinition(namespace.getName(), storageOptions);
    }

    @AllArgsConstructor
    @Builder
    @Getter
    public static class StorageOptions {
        private final boolean enabled;
        private final boolean dropOnFileFormatError;
        private final boolean createIfMissing;
    }

}
