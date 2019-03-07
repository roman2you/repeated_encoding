package com.icongtai.zebra.encoding.values;

import com.google.common.collect.Maps;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.GZipUtils;
import com.icongtai.zebra.encoding.config.Column;
import com.icongtai.zebra.encoding.config.ColumnConfig;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.format.ColumnType;
import com.icongtai.zebra.encoding.format.DataGroupSchema;
import com.icongtai.zebra.encoding.format.DataRecord;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.delta.DeltaLongBitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.deltaprefixnumber.DeltaPrefixIntegerReader;
import com.icongtai.zebra.encoding.impl.ext.GpsBearingValuesReader;
import com.icongtai.zebra.encoding.impl.ext.GpsSpeedValuesReader;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DataReaderV1 {

    private byte[] body;

    private byte[] message;

    private DataGroupSchema schema;

    private Map<Column, ColumnReader> columnReaderMap = Maps.newHashMap();

    private int offset;


    public DataReaderV1(byte[] message) {
        this.message = message;
    }

    public void initMessage() throws IOException {
        this.schema = DataGroupSchema.from(message);
        BytesInput messageBody = BytesInput.from(message, schema.getDataOffset(), message.length - schema.getDataOffset());
        this.body = messageBody.toByteArray();
        if(this.schema.isCompressed()) {
            try {
                this.body = GZipUtils.decompress(this.body);
            } catch (Exception e) {
                throw new ZebraDecodingException("body decompressed fail");
            }
        }
        List<ColumnConfig> columnConfigs = this.schema.getColumnConfigs();
        for (ColumnConfig columnConfig : columnConfigs) {
            getColumnReader(columnConfig);
        }

    }

    public ColumnReader getColumnReader(ColumnConfig columnConfig) {
        if (columnConfig == null) {
            throw new ZebraDecodingException("exist column config is null, column=" + columnConfig);
        }
        ColumnReader reader = columnReaderMap.get(columnConfig);
        if (reader != null) {
            return reader;
        }
        EncodeType encodeType = columnConfig.getEncodeType();
        try {
            if (encodeType == EncodeType.bearing_encoding) {
                reader = new ColumnReadeV1(new GpsBearingValuesReader(), this.body, schema.getRows(), offset, columnConfig);
            } else if (encodeType == EncodeType.delta) {
                ColumnType columnType = columnConfig.getColumnType();
                if (columnType == ColumnType.LONG || columnType == ColumnType.UNSIGNED_LONG) {
                    reader = new ColumnReadeV1(new DeltaLongBitPackingValuesReader(), this.body, schema.getRows(), offset, columnConfig);
                } else {
                    reader = new ColumnReadeV1(new DeltaLongBitPackingValuesReader(), this.body, schema.getRows(), offset, columnConfig);
                }
            } else if (encodeType == EncodeType.delta_prefix_int) {
                reader = new ColumnReadeV1(new DeltaPrefixIntegerReader(false, 4), this.body, schema.getRows(), offset, columnConfig);
            } else if (encodeType == EncodeType.rle) {
                reader = new ColumnReadeV1(new RunLengthBitPackingHybridValuesReader(false), this.body, schema.getRows(), offset, columnConfig);
            } else if (encodeType == EncodeType.speed_encoding) {
                reader = new ColumnReadeV1(new GpsSpeedValuesReader(), this.body, schema.getRows(), offset, columnConfig);
            }
        } catch (IOException e) {
            throw new ZebraEncodingException("data error, init column reader fail, column=" + columnConfig.getName());
        }
        offset = reader.getNextOffset();
        columnReaderMap.put(columnConfig.getColumn(), reader);
        return reader;
    }

    public DataReaderV1 readData(Column column) {
        ColumnReader reader =  columnReaderMap.get(column);
        if(reader == null) {
            throw  new ZebraDecodingException("not exist column or no init column=" + column);
        }

        return this;
    }

    public DataGroupSchema getSchema() {
        return this.schema;
    }

    public DataRecord getRecord() {
        return new DataRecord(schema, columnReaderMap);
    }

}
