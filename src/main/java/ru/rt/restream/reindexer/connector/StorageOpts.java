package ru.rt.restream.reindexer.connector;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class StorageOpts {
    private final boolean enableStorage;
    private final boolean dropOnFileFormatError;
    private final boolean createIfMissing;
}
