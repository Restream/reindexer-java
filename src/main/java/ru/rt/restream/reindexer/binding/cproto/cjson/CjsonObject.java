package ru.rt.restream.reindexer.binding.cproto.cjson;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A class representing an object type in Cjson. An object consists of name-value pairs where names
 * are strings, and values are any other type of {@link CjsonElement}. This allows for a creating a
 * tree of CjsonElements.
 */
public class CjsonObject extends CjsonElement {

    private final Map<String, CjsonElement> members = new HashMap<>();

    public void add(String name, CjsonElement element) {
        if (element == null) {
            element = CjsonNull.INSTANCE;
        }

        members.put(name, element);
    }

    public CjsonElement getProperty(String name) {
        return members.getOrDefault(name, CjsonNull.INSTANCE);
    }

    public Set<Map.Entry<String, CjsonElement>> entries() {
        return members.entrySet();
    }

}
