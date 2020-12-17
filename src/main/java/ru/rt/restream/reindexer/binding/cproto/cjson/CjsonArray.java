package ru.rt.restream.reindexer.binding.cproto.cjson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class represents an array type in Cjson. An array is a list of {@link CjsonElement}s each of
 * which can be of a different type.
 */
public class CjsonArray extends CjsonElement implements Iterable<CjsonElement> {

    private final List<CjsonElement> members = new ArrayList<>();

    /**
     * Adds the specified element to self.
     *
     * @param element the element that needs to be added to the array.
     */
    public void add(CjsonElement element) {
        if (element == null) {
            element = CjsonNull.INSTANCE;
        }

        members.add(element);
    }

    @Override
    public Iterator<CjsonElement> iterator() {
        return members.iterator();
    }
}
