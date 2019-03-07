package com.icongtai.zebra.encoding.config;

import com.icongtai.zebra.encoding.format.ColumnType;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.format.StructType;
import lombok.Data;

/**
 * Created by robin on 18/10/16.
 */
@Data
public class ColumnConfig {
    private int id;

    private String name;

    private EncodeType encodeType;

    private ColumnType columnType;

    private StructType structType;

    public Column getColumn() {
        return Column.valueOf(name);
    }

}
