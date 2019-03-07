package com.icongtai.zebra.encoding.format;

import com.icongtai.zebra.encoding.config.Column;
import com.icongtai.zebra.encoding.config.ColumnConfig;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;
import com.icongtai.zebra.encoding.values.ColumnReader;

import java.util.*;

/**
 * Created by robin on 20/10/16.
 */
public class DataRecord {

    private Map<Column, Integer> columnIndex;

    private Map<Column, ColumnReader> columnReaderMap;

    private List<Object[]> table;

    private Object[] currentRow;

    private Iterator<Object[]> iterator;

    private DataGroupSchema schema;

    public DataRecord(DataGroupSchema schema, Map<Column, ColumnReader> columnColumnReaderMap) {
        table = new ArrayList<>(schema.getRows());
        columnIndex = new HashMap<>();
        int i = 0;
        for(ColumnConfig columnConfig : schema.getColumnConfigs()) {
            columnIndex.put(columnConfig.getColumn(), i++);
        }
        for(i = 0; i < schema.getRows(); i++) {
            table.add(new Object[columnIndex.size()]);
        }
        iterator = table.iterator();
        this.columnReaderMap = columnColumnReaderMap;
        this.schema = schema;
    }

    public boolean hasNext() {
        if(iterator.hasNext()) {
            return true;
        }
        return false;
    }

    public DataRecord next() {
        if(hasNext()) {
            currentRow =  iterator.next();
            int i = 0;
            for(ColumnConfig columnConfig : schema.getColumnConfigs()) {
                Column column = columnConfig.getColumn();
                ColumnReader reader = columnReaderMap.get(column);
                if(reader == null) {
                    throw new ZebraDecodingException("column not init, column=" + column);
                }
                ColumnType type = columnConfig.getColumnType();
                switch (type) {
                    case BOOL:
                        currentRow[i++] = reader.getBoolean();
                        break;
                    case BYTE:
                    case SHORT:
                    case INT:
                    case UNSIGNED_INT:
                    case GPS_BEARING:
                    case GPS_LL:
                    case GPS_SPEED:
                        currentRow[i++] =  reader.getInteger();
                        break;
                    case LONG:
                    case UNSIGNED_LONG:
                        currentRow[i++] = reader.getLong();
                        break;
                    case FLOAT:
                        currentRow[i++] =  reader.getFloat();
                        break;
                    case DOUBLE:
                        currentRow[i++] = reader.getDouble();
                        break;
                    case STRING:
                        currentRow[i++] =  reader.getBinary();
                        break;
                }

            }
        } else {
            throw new NoSuchElementException();
        }
        return this;
    }

    public <T> T getValue(Column column, ColumnType columnType) {
        if(!columnIndex.containsKey(column)) {
            throw  new ZebraDecodingException("not exist this column " + column);
        }
        Object v = currentRow[columnIndex.get(column)];
        if(v == null) {
            return null;
        }
        if(column == Column.speed || column == Column.bearing) {
            Object f = (int)v / 10.0f;
            return (T) f;
        }
        if(column == Column.latitude || column == Column.longitude) {
            Object f = (int)v / 1000000.0d;
            return (T) f;
        }
        if(columnType.getClazz().isInstance(v)) {
            return (T) v;
        }
        throw new ZebraDecodingException("Type not match, type = " + columnType.getClazz() + ", realType=" + v.getClass());
    }

}
