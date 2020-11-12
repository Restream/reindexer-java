package ru.rt.restream.reindexer.connector.binding.def;

import lombok.Getter;
import lombok.Setter;
import ru.rt.restream.reindexer.connector.StorageOpts;

@Getter
@Setter
public class NamespaceDef {

    private final String name;
    private final StorageOpts storage;
    //"indexes":[]

    public NamespaceDef(StorageOpts storage, String name) {
        this.name = name;
        this.storage = storage;
    }
}
