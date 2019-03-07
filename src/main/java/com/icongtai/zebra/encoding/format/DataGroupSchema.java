package com.icongtai.zebra.encoding.format;

import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.config.ColumnConfig;
import com.icongtai.zebra.encoding.config.ColumnConfigStore;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter;
import com.icongtai.zebra.encoding.impl.varplain.VarPlainValuesReader;
import com.icongtai.zebra.encoding.impl.varplain.VarPlainValuesWriter;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class DataGroupSchema {

    private int version;

    private boolean compressed;

    private int rows;

    private int dataOffset;

    private List<ColumnConfig> columnConfigs;


    public void setVersion(int version) {
        this.version = version;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public void setDataOffset(int dataOffset) {
        this.dataOffset = dataOffset;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public int getVersion() {
        return version;
    }

    public int getRows() {
        return rows;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public int getDataOffset() {
        return dataOffset;
    }

    public void setColumnConfigs(List<ColumnConfig> columnConfigs) {
        this.columnConfigs = columnConfigs;
    }

    public List<ColumnConfig> getColumnConfigs() {
        return columnConfigs;
    }

    public BytesInput getBytes() {
        DeltaIntBitPackingValuesWriter columnsWriter = new DeltaIntBitPackingValuesWriter(32, 1, 32);
        for(ColumnConfig columnConfig : columnConfigs) {
            columnsWriter.writeInteger(columnConfig.getId());
        }

        BytesInput columnSchemaHead = columnsWriter.getBytes();
        if(CollectionUtils.isEmpty(columnConfigs)) {
            throw  new ZebraEncodingException("this encoding not select columns");
        }
        VarPlainValuesWriter writer = new VarPlainValuesWriter(16, false);
        writer.writeInteger(version << 1 | (isCompressed() ? 1 : 0));
        writer.writeInteger(rows);
        long size = columnSchemaHead.size() + writer.getBufferedSize();
        if(size < Byte.MAX_VALUE) {
            dataOffset = (int) (size + 1);
        } else {
            dataOffset = (int) (size + 2);
        }
        writer.writeInteger(dataOffset);

        BytesInput head = writer.getBytes();


        return BytesInput.concat(columnSchemaHead, head);
    }

    public static DataGroupSchema from(byte[] message) throws IOException {
        DeltaIntBitPackingValuesReader reader = new DeltaIntBitPackingValuesReader();
        reader.init(16, message, 0);
        int idCount = reader.getTotalValueCount();
        DataGroupSchema schema = new DataGroupSchema();
        List<ColumnConfig> columnConfigs = new ArrayList<>();
        for(int i = 0; i < idCount ; i++) {
            ColumnConfig columnConfig = ColumnConfigStore.getInstance().getById(reader.readInteger());
            columnConfigs.add(columnConfig);
        }
        schema.setColumnConfigs(columnConfigs);
        VarPlainValuesReader varReader = new VarPlainValuesReader.VarIntegerPlainValuesReader(false);
        varReader.init(4, message, reader.getNextOffset());
        int version = varReader.readInteger();
        schema.setVersion((byte) (version >>> 1));
        schema.setCompressed((version & 1) == 1 ? true : false);
        schema.setRows(varReader.readInteger());
        schema.setDataOffset(varReader.readInteger());
        return schema;
    }
}
