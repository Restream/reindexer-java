package ru.rt.restream.reindexer.connector;

import ru.rt.restream.reindexer.connector.binding.Binding;
import ru.rt.restream.reindexer.connector.binding.Consts;
import ru.rt.restream.reindexer.connector.binding.QueryResult;
import ru.rt.restream.reindexer.connector.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.connector.binding.cproto.CprotoIterator;

import java.util.Iterator;

import static ru.rt.restream.reindexer.connector.binding.Consts.OP_AND;

//// Query to DB object
//type Query struct {
//        Namespace       string
//        db              *reindexerImpl
//        nextOp          int
//        ser             cjson.Serializer
//        root            *Query
//        joinQueries     []*Query
//        mergedQueries   []*Query
//        joinToFields    []string
//        joinHandlers    []JoinHandler
//        context         interface{}
//        joinType        int
//        closed          bool
//        initBuf         [256]byte
//        nsArray         []nsArrayEntry
//        ptVersions      []int32
//        iterator        Iterator
//        jsonIterator    JSONIterator
//        items           []interface{}
//        json            []byte
//        jsonOffsets     []int
//        totalName       string
//        executed        bool
//        fetchCount      int
//        queriesCount    int
//        opennedBrackets []int
//        tx              *Tx
//        }
public class Query<T> {

    private final Binding binding;

    private final ByteBuffer buffer = new ByteBuffer();

    private int nextOperation = OP_AND;

    private final Class<T> itemClass;

    private int count;

    public Query(Binding binding, String namespace, Class<T> itemClass) {
        this.binding = binding;
        this.itemClass = itemClass;
        buffer.writeString(namespace);
    }

    public Query<T> join(String joined) {
        return null;
    }

    public Query<T> where(String index, int condition, Object... values) {
        buffer.writeUnsignedVarInt(Consts.QUERY_CONDITION)
                .writeString(index)
                .writeUnsignedVarInt(nextOperation)
                .writeUnsignedVarInt(condition);

        this.nextOperation = OP_AND;
        count++;

        if (values != null && values.length > 0) {
            buffer.writeUnsignedVarInt(values.length);
            for (Object key : values) {
                putValue(key);
            }
        }

        return this;
    }

    private void putValue(Object value) {
        if (value instanceof Integer) {
            buffer.writeUnsignedVarInt(Consts.VALUE_INT)
                    .writeVarInt((Integer) value);
        } else if (value instanceof String) {
            buffer.writeUnsignedVarInt(Consts.VALUE_STRING)
                    .writeString((String) value);
        }
    }

    public Iterator<T> execute() {
        //get namespace
        buffer.writeUnsignedVarInt(Consts.QUERY_END);

        QueryResult queryResult = binding.selectQuery(buffer.bytes(), true, Integer.MAX_VALUE);

        return CprotoIterator.<T>builder()
                .itemClass(itemClass)
                .queryResult(queryResult)
                .build();
    }

}
