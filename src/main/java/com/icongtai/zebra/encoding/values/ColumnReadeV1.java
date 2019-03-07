package com.icongtai.zebra.encoding.values;

import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.config.ColumnConfig;
import com.icongtai.zebra.encoding.format.StructType;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesReader;

import java.io.IOException;

/**
 *
 */
public class ColumnReadeV1 implements ColumnReader {

    private Log LOG = Log.getLog(ColumnReadeV1.class);

    private ValuesReader valuesReader;

    public ColumnConfig columnConfig;

    private final int totalValueCount;
    private int nullCount;
    private final byte[] definitionLevels;
    private int readBufferCount;

    private int nextOffset;

    public ColumnReadeV1(ValuesReader reader, byte[] body, int totalValueCount, int offset, ColumnConfig columnConfig) throws IOException {
        this.valuesReader = reader;
        this.columnConfig = columnConfig;
        this.totalValueCount = totalValueCount;
        definitionLevels = new byte[totalValueCount];
        if (columnConfig.getStructType() == StructType.optional) {
            RunLengthBitPackingHybridValuesReader definitionReader = new RunLengthBitPackingHybridValuesReader(false);
            definitionReader.init(totalValueCount, body, offset);
            nextOffset = definitionReader.getNextOffset();
            nullCount = 0;
            for (int i = 0; i < totalValueCount; i++) {
                byte definition = (byte) definitionReader.readInteger();
                if (definition == 0) {
                    ++nullCount;
                }
                definitionLevels[i] = definition;
            }

        } else {
            nextOffset = offset;
        }
        valuesReader.init(this.totalValueCount - nullCount, body, nextOffset);
        LOG.debug(columnConfig.getColumn() + " init success, size=" + (valuesReader.getNextOffset() - offset));
        nextOffset = valuesReader.getNextOffset();
    }


    @Override
    public int getNextOffset() {
        return nextOffset;
    }

    @Override
    public int getCurrentDefinitionLevel() {
        if (columnConfig.getStructType() == StructType.optional) {
            return definitionLevels[readBufferCount++];
        }
        return 1;
    }

    @Override
    public void skip() {
        valuesReader.skip();
    }

    @Override
    public Integer getInteger() {
        int level = getCurrentDefinitionLevel();
        if (level > 0) {
            return valuesReader.readInteger();
        }
        return null;
    }

    @Override
    public boolean getBoolean() {
        getCurrentDefinitionLevel();
        return valuesReader.readBoolean();
    }

    @Override
    public Long getLong() {
        int level = getCurrentDefinitionLevel();
        if (level > 0) {
            return valuesReader.readLong();
        }
        return null;
    }

    @Override
    public Binary getBinary() {
        int level = getCurrentDefinitionLevel();
        if (level > 0) {
            return valuesReader.readBytes();
        }
        return null;
    }

    @Override
    public Float getFloat() {
        int level = getCurrentDefinitionLevel();
        if (level > 0) {
            return valuesReader.readFloat();
        }
        return null;
    }

    @Override
    public Double getDouble() {
        int level = getCurrentDefinitionLevel();
        if (level > 0) {
            return valuesReader.readDouble();
        }
        return null;
    }

    @Override
    public ColumnConfig getColumnConfig() {
        return columnConfig;
    }

    @Override
    public ValuesReader getDataReader() {
        return valuesReader;
    }
}
