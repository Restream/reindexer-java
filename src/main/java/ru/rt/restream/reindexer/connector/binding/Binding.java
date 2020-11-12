package ru.rt.restream.reindexer.connector.binding;

import ru.rt.restream.reindexer.connector.binding.def.IndexDef;

public interface Binding {

    void openNamespace(String namespace, boolean enableStorage, boolean dropOnFileFormatError);

    void addIndex(String namespace, IndexDef index);

    void modifyItem(int nsHash, String namespace, int format, byte[] data, int mode, String[] percepts, int stateToken);

    void dropNamespace(String namespace);

    void closeNamespace(String namespace);

}
