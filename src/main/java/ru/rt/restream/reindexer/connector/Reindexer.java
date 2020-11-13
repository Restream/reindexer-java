package ru.rt.restream.reindexer.connector;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.connector.binding.Binding;
import ru.rt.restream.reindexer.connector.binding.Consts;
import ru.rt.restream.reindexer.connector.binding.cproto.Serializer;
import ru.rt.restream.reindexer.connector.binding.def.IndexDef;
import ru.rt.restream.reindexer.connector.exceptions.IndexConflictException;
import ru.rt.restream.reindexer.connector.exceptions.NsExistsException;
import ru.rt.restream.reindexer.connector.exceptions.ReindexerException;
import ru.rt.restream.reindexer.connector.exceptions.UnimplementedException;
import ru.rt.restream.reindexer.connector.options.IndexOptions;
import ru.rt.restream.reindexer.connector.options.NamespaceOptions;
import ru.rt.restream.reindexer.util.Pair;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Reindexer {

    private final Binding binding;

    private final Map<String, Namespace<?>> namespaceMap = new ConcurrentHashMap<>();

    Reindexer(Binding binding) {
        this.binding = binding;
    }

    public <T> void openNamespace(String namespace, NamespaceOptions options, Class<T> clazz) {
        registerNamespace(namespace, options, clazz);
        Namespace<T> ns = getNs(namespace, clazz);
        try {
            binding.openNamespace(ns.getName(), options.isEnableStorage(), options.isDropOnFileFormatError());

            for (IndexDef index : ns.getIndexes()) {
                binding.addIndex(ns.getName(), index);
            }
        } catch (IndexConflictException e) {
            if (options.isDropOnIndexConflict()) {
                binding.dropNamespace(namespace);
            } else {
                binding.closeNamespace(ns.getName());
            }
        } catch (Exception e) {
            binding.closeNamespace(ns.getName());
        }

    }

    public <T> void upsert(String namespace, T item, String... precepts) {
        Class<T> clazz = (Class<T>) item.getClass();
        Namespace<T> ns = getNs(namespace, clazz);
        modifyItem(ns, item, null, Consts.MODE_UPSERT, precepts);
    }

    public Query<?> query(String namespace) {
        return new Query<>();
    }

    private <T> Namespace<T> getNs(String namespace, Class<T> clazz) {
        String name = namespace.toLowerCase();
        Namespace<?> ns = namespaceMap.get(name);
        if (ns.getClazz() != clazz) {
            throw new RuntimeException("Wrong namespace type");
        }

        return (Namespace<T>) ns;
    }

    private <T> void registerNamespace(String namespace, NamespaceOptions options, Class<T> clazz) {

        String name = namespace.toLowerCase();
        Namespace<T> ns = new Namespace<>(name, clazz);
        if (ns.equals(namespaceMap.get(name))) {
            throw new NsExistsException();
        }

        ns.setOptions(options);
        ns.setIndexes(parseIndex(clazz, ns.getJoined()));
        namespaceMap.put(name, ns);
    }

    private List<IndexDef> parseIndex(Class<?> clazz, Map<String, int[]> joined) {
        List<IndexDef> indexDefs = parse(clazz, false, "", "", joined);
        return new ArrayList<>(indexDefs);
    }

    private <T> List<IndexDef> parse(Class<T> clazz, boolean subArray, String aReindexBasePath,
                                     String aJsonBasePath, Map<String, int[]> joined) {
        String jsonBasePath = aJsonBasePath;
        if (jsonBasePath.length() != 0 && !jsonBasePath.endsWith(".")) {
            jsonBasePath += ".";
        }

        String reindexBasePath = aReindexBasePath;
        if (reindexBasePath.length() != 0 && !reindexBasePath.endsWith(".")) {
            reindexBasePath += ".";
        }

        List<IndexDef> indexDefs = new ArrayList<>();
        for (Field field : clazz.getDeclaredFields()) {
            Reindex reindex = field.getDeclaredAnnotation(Reindex.class);
            String[] tagsSlice = reindex.value().split(",", 3);
            String jsonPath = ""; // TODO @Json annotation
            if (jsonPath.length() == 0) {
                jsonPath = field.getName();
            }
            jsonPath = jsonBasePath + jsonPath;

            String idxName = tagsSlice[0];
            String idxType = "";
            String idxOpts = "";

            if ("-".equals(idxName)) {
                continue;
            }

            if (tagsSlice.length > 1) {
                idxType = tagsSlice[1];
            }
            if (tagsSlice.length > 2) {
                idxOpts = tagsSlice[2];
            }

            String reindexPath = reindexBasePath + idxName;
            List<String> idxSettings = splitOptions(idxOpts);
            Pair<IndexOptions, List<String>> options = parseOptions(idxSettings);
            IndexOptions indexOptions = options.getFirst();
            idxSettings = options.getSecond();

            if (indexOptions.isPk() && idxName.trim().isEmpty()) {
                String message = String.format("No index name is specified for primary key in field %s",
                        field.getName());
                throw new ReindexerException(message);
            }
            if (idxName.length() > 0) {
                //TODO: collateMode, sortOrderLetters := parseCollate(&idxSettings)
                String fieldType = getFieldType(field.getGenericType());
                IndexDef indexDef = IndexDef.makeIndexDef(reindexPath, Arrays.asList(jsonPath),
                        idxType, fieldType, options.getFirst(), Consts.COLLATE_NONE, "");
                indexDefs.add(indexDef);
            }

            if (idxSettings.size() > 0) {
                throw new ReindexerException(String.format("Unknown index settings are found: %s", idxSettings));
            }
        }

        return indexDefs;
    }

    private String getFieldType(Type genericType) {
        switch (genericType.getTypeName()) {
            case "boolean":
            case "java.lang.Boolean":
                return "bool";
            case "byte":
            case "java.lang.Byte":
            case "short":
            case "java.lang.Short":
            case "int":
            case "java.lang.Integer":
                return "int";
            case "java.lang.Long":
                return "int64";
            case "java.lang.String":
                return "string";
            case "java.lang.Double":
            case "java.lang.Float":
                return "double";
            default:
                throw new ReindexerException("Invalid generic type");
        }
    }

    private Pair<IndexOptions, List<String>> parseOptions(List<String> idxSettings) {
        List<String> newIdxSettingsBuf = new ArrayList<>();

        IndexOptions.IndexOptionsBuilder optionsBuilder = IndexOptions.builder();

        for (String idxSetting : idxSettings) {
            if ("pk".equals(idxSetting)) {
                optionsBuilder.isPk(true);
            } else if ("dense".equals(idxSetting)) {
                optionsBuilder.isDense(true);
            } else if ("sparse".equals(idxSetting)) {
                optionsBuilder.isSparse(true);
            } else if ("appendable".equals(idxSetting)) {
                optionsBuilder.isAppendable(true);
            } else {
                newIdxSettingsBuf.add(idxSetting);
            }
        }

        return new Pair<>(optionsBuilder.build(), newIdxSettingsBuf);
    }

    private List<String> splitOptions(String indexOptions) {
        List<String> words = new ArrayList<>();
        StringBuilder word = new StringBuilder();
        int strLen = indexOptions.length();
        int i = 0;

        while (i < strLen) {

            if ('\\' == indexOptions.charAt(i) && i < strLen - 1 && ',' == indexOptions.charAt(i + 1)) {
                word.append(indexOptions.charAt(i + 1));
                i += 2;
                continue;
            }

            if (indexOptions.charAt(i) == ',') {
                words.add(word.toString());
                word = new StringBuilder();
                i++;
                continue;
            }

            word.append(indexOptions.charAt(i));

            if (i == strLen - 1) {
                words.add(word.toString());
                word = new StringBuilder();
                i++;
                continue;
            }
            i++;
        }

        return words;
    }


    private static class PackItemResult {
        int format = 0;
        int stateToken = 0;
    }

    private <T> PackItemResult packItem(Namespace<T> ns, T item, ByteBuffer json, Serializer ser) {

        if (!ns.getClazz().equals(item.getClass())) {
            throw new IllegalArgumentException(); // TODO ErrWrongType
        }

        PackItemResult res = new PackItemResult();

        String sJson = null;
        if (item != null) {
            // json, _ = item.([]byte)
            sJson = toJson(item);
        }

        if (sJson == null) {
            throw new UnimplementedException();
            /*t := reflect.TypeOf(item)
            if t.Kind() == reflect.Ptr {
                t = t.Elem()
            }
            if ns.rtype.Name() != t.Name() {
                panic(ErrWrongType)
            }

            format = bindings.FormatCJson

            enc := ns.cjsonState.NewEncoder()
            if stateToken, err = enc.Encode(item, ser); err != nil {
                return
            }*/
        } else {
            res.format = Consts.FORMAT_JSON;
            ser.writeBytes(sJson.getBytes());
        }
        return res;
    }

    private <T> void modifyItem(Namespace<T> ns, T item, ByteBuffer json, int mode, String... precepts) {
        Serializer serializer = new Serializer();
        PackItemResult pack = packItem(ns, item, json, serializer);

        binding.modifyItem(ns.hashCode(), ns.getName(), pack.format, serializer.bytes(), mode,
                precepts, pack.stateToken);
    }

    private String toJson(Object object) {
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
        return gson.toJson(object);
    }
}

