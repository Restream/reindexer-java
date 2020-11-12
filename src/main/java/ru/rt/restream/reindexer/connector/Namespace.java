package ru.rt.restream.reindexer.connector;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import ru.rt.restream.reindexer.connector.binding.def.IndexDef;
import ru.rt.restream.reindexer.connector.options.NamespaceOptions;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@EqualsAndHashCode(of = {"name", "clazz"})
public class Namespace<T> {
    private String name;
    private Class<T> clazz;
    private NamespaceOptions options;
    private List<IndexDef> indexes;
    private Map<String, int[]> joined;
    private boolean opened;

    public Namespace(String namespace, Class<T> clazz) {
        this.name = namespace;
        this.clazz = clazz;
    }

}


