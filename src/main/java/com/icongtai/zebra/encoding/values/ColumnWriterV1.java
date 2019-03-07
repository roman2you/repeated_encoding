package com.icongtai.zebra.encoding.values;

import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.config.ColumnConfig;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.format.ColumnType;
import com.icongtai.zebra.encoding.format.StructType;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesWriter;

/**
 */
public class ColumnWriterV1 implements ColumnWriter {

    private Log LOG = Log.getLog(ColumnWriterV1.class);

    private RunLengthBitPackingHybridValuesWriter definitionLevelWriter;

    private ValuesWriter dataWriter;

    private ColumnConfig columnConfig;

    public ColumnWriterV1(ValuesWriter dataWriter, ColumnConfig columnConfig) {
        this.dataWriter = dataWriter;
        this.columnConfig = columnConfig;
        if (columnConfig.getStructType() == StructType.optional) {
            definitionLevelWriter = new RunLengthBitPackingHybridValuesWriter(1, 0, 128);
        }
    }

    @Override
    public void write(int value) {
        if(isCheckUnSignedType(columnConfig.getColumnType())) {
            if(value < 0) {
                writeNull(0);
                return;
            }
        }
        if (columnConfig.getStructType() == StructType.optional) {
            definitionLevelWriter.writeInteger(1);
        }
        dataWriter.writeInteger(value);
    }

    private boolean isCheckUnSignedType(ColumnType columnType) {
        if (columnConfig.getColumnType() == ColumnType.UNSIGNED_INT || columnConfig.getColumnType() == ColumnType.UNSIGNED_LONG || columnConfig.getColumnType() == ColumnType.GPS_SPEED || columnConfig.getColumnType() == ColumnType.GPS_BEARING) {
            return true;
        }
        return false;
    }

    @Override
    public void write(long value) {
        if(isCheckUnSignedType(columnConfig.getColumnType())) {
            if(value < 0) {
                writeNull(0);
                return;
            }
        }
        if (columnConfig.getStructType() == StructType.optional) {
            definitionLevelWriter.writeInteger(1);
        }
        dataWriter.writeLong(value);
    }

    @Override
    public void write(boolean value) {
        if (columnConfig.getStructType() == StructType.optional) {
            definitionLevelWriter.writeInteger(1);
        }
        dataWriter.writeInteger(value ? 1 : 0);
    }

    @Override
    public void write(Binary value) {
        if (columnConfig.getStructType() == StructType.optional) {
            definitionLevelWriter.writeInteger(1);
        }
        dataWriter.writeBytes(value);
    }

    @Override
    public void write(float value) {
        if(isCheckUnSignedType(columnConfig.getColumnType())) {
            if(value < 0) {
                writeNull(0);
                return;
            }
        }
        if (columnConfig.getStructType() == StructType.optional) {
            definitionLevelWriter.writeInteger(1);
        }
        dataWriter.writeFloat(value);
    }

    @Override
    public void write(double value) {
        if(isCheckUnSignedType(columnConfig.getColumnType())) {
            if(value < 0) {
                writeNull(0);
                return;
            }
        }
        if (columnConfig.getStructType() == StructType.optional) {
            definitionLevelWriter.writeInteger(1);
        }
        dataWriter.writeDouble(value);
    }

    @Override
    public void writeNull(int definitionLevel) {
        if (columnConfig.getStructType() == StructType.required) {
            throw new ZebraEncodingException("StructType required not support null value");
        }
        definitionLevelWriter.writeInteger(definitionLevel);
    }

    @Override
    public BytesInput getBytes() {
        if (columnConfig.getStructType() == StructType.optional) {
            BytesInput definitionBytes = definitionLevelWriter.getBytes();
            BytesInput bytesInput =  BytesInput.concat(definitionBytes, dataWriter.getBytes());
            LOG.debug(columnConfig.getColumn() + ">> encoding buffer size = " + bytesInput.size() + " definition buffer size=" + definitionBytes.size());
            return bytesInput;
        }
        BytesInput bytesInput = dataWriter.getBytes();
        LOG.debug(columnConfig.getColumn() + ">> encoding buffer size = " + bytesInput.size());
        return bytesInput;
    }
}
