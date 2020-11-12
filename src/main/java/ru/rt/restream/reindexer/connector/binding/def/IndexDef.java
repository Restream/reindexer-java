package ru.rt.restream.reindexer.connector.binding.def;

import lombok.AllArgsConstructor;
import lombok.Getter;
import ru.rt.restream.reindexer.connector.binding.Consts;
import ru.rt.restream.reindexer.connector.options.IndexOptions;
import ru.rt.restream.reindexer.util.Pair;

import java.util.List;

/*type IndexDef struct {
	Name        string      `json:"name"`
	JSONPaths   []string    `json:"json_paths"`
	IndexType   string      `json:"index_type"`
	FieldType   string      `json:"field_type"`
	IsPK        bool        `json:"is_pk"`
	IsArray     bool        `json:"is_array"`
	IsDense     bool        `json:"is_dense"`
	IsSparse    bool        `json:"is_sparse"`
	CollateMode string      `json:"collate_mode"`
	SortOrder   string      `json:"sort_order_letters"`
	Config      interface{} `json:"config"`
}
*/
@AllArgsConstructor
@Getter
public class IndexDef {

    private final String name;

    private final List<String> jsonPaths;

    private final String indexType;

    private final String fieldType;

    private final boolean isPk;

    private final boolean isArray;

    private final boolean isDense;

    private final boolean isSparse;

    private final String collateMode;

    private final String sortOrder;

    public static IndexDef makeIndexDef(String reindexPath, List<String> jsonPaths, String idxType, String fieldType,
                                        IndexOptions indexOptions, int collate, String sortOrder) {
        String cm = "";
        switch (collate) {
            case Consts.COLLATE_ASCII:
                cm = "ascii";
                break;
            case Consts.COLLATE_UTF_8:
                cm = "utf8";
                break;
            case Consts.COLLATE_NUMERIC:
                cm = "numeric";
                break;
            case Consts.COLLATE_CUSTOM:
                cm = "custom";
                break;
        }

        return new IndexDef(
                reindexPath,
                jsonPaths,
                idxType,
                fieldType,
                indexOptions.isPk(),
                indexOptions.isArray(),
                indexOptions.isDense(),
                indexOptions.isSparse(),
                cm,
                sortOrder
        );
    }
}
