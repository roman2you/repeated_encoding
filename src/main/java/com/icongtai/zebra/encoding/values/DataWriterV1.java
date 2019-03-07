package com.icongtai.zebra.encoding.values;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.GZipUtils;
import com.icongtai.zebra.encoding.config.Column;
import com.icongtai.zebra.encoding.config.ColumnConfig;
import com.icongtai.zebra.encoding.config.ColumnConfigStore;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.format.ColumnType;
import com.icongtai.zebra.encoding.format.DataGroupSchema;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter;
import com.icongtai.zebra.encoding.impl.delta.DeltaLongBitPackingValuesWriter;
import com.icongtai.zebra.encoding.impl.deltaprefixnumber.DeltaPrefixIntegerWriter;
import com.icongtai.zebra.encoding.impl.ext.GpsBearingValuesWriter;
import com.icongtai.zebra.encoding.impl.ext.GpsSpeedValuesWriter;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.commons.collections.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by robin on 20/10/16.
 */
public class DataWriterV1 {
    private LinkedHashMap<Column, ColumnConfig> columnConfigs = new LinkedHashMap<>();

    private Map<Column, List> dataMap = Maps.newHashMap();

    private Map<Column, List> maxAndMinValues = Maps.newHashMap();

    private int totalCount;
    private BytesInput message;

    public DataWriterV1 select(Column column) {
        ColumnConfig columnConfig = ColumnConfigStore.getInstance().getByName(column.name());
        if (columnConfig == null) {
            throw new RuntimeException("Not found this column config");
        }
        if (columnConfigs.containsKey(column)) {
            throw new RuntimeException("Repeated column , column=" + column.name());
        }

        columnConfigs.put(column, columnConfig);

        return this;
    }

    public void writeRecord(Map<Column, Object> rowData) {
        for(Map.Entry<Column, ColumnConfig> entry : columnConfigs.entrySet()) {
            if(!rowData.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("rowData not contains column, column=" + entry.getKey());
            }
            List datas  = dataMap.get(entry.getKey());
            if(datas == null) {
                datas = new LinkedList<>();
                dataMap.put(entry.getKey(), datas);
            }
            Object o  = rowData.get(entry.getKey());
            datas.add(o);
        }
        ++totalCount;
    }

    public void flush(boolean gzip) {
        DataGroupSchema schema = new DataGroupSchema();
        schema.setColumnConfigs(Lists.newArrayList(columnConfigs.values()));
        schema.setCompressed(gzip);
        schema.setRows(totalCount);
        schema.setVersion(1);
        BytesInput body = null;
        for(ColumnConfig columnConfig : columnConfigs.values()) {
            ColumnType type = columnConfig.getColumnType();
            Column column = columnConfig.getColumn();

            prepareDatas(column);

            ColumnWriter writer = new ColumnWriterV1(getValuesWriter(column), columnConfig);

            List<Object> datas = dataMap.get(column);
            for (Object data : datas) {
                if (data == null) {
                    writer.writeNull(0);
                } else {
                    switch (type) {
                        case BOOL:
                            writer.write((Boolean) data);
                            break;
                        case BYTE:
                        case SHORT:
                        case INT:
                        case UNSIGNED_INT:
                        case GPS_BEARING:
                        case GPS_LL:
                        case GPS_SPEED:
                            writer.write((Integer) data);
                            break;
                        case LONG:
                        case UNSIGNED_LONG:
                            writer.write((Long) data);
                            break;
                        case FLOAT:
                            writer.write((Float) data);
                            break;
                        case DOUBLE:
                            writer.write((Double) data);
                            break;
                        case STRING:
                            writer.write((Binary) data);
                            break;
                    }
                }
            }
            System.out.println("");
            if(body == null) {
                body = writer.getBytes();
            } else {
                body = BytesInput.concat(body, writer.getBytes());
            }
        }

        if(gzip) {
            try {
                body = BytesInput.from(GZipUtils.compress(body.toByteArray()));
            } catch (Exception e) {
                throw new ZebraEncodingException("body compress fail", e);
            }
        }
        this.message = BytesInput.concat(schema.getBytes(), body);
    }

    public BytesInput getMessage() {
        return message;
    }

    private void prepareDatas(Column column) {
        List<Object> datas = dataMap.get(column);
        ColumnConfig columnConfig = columnConfigs.get(column);
        ColumnType type = columnConfig.getColumnType();
        if(type == ColumnType.GPS_LL) {
            List<Integer> list = Lists.newArrayList();
            int factor = 1000000;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            for(Object data : datas) {
                double dll = (Double)data;
                int ill = (int) (dll * factor);
                if (ill > max) {
                    max = ill;
                }
                if (ill < min) {
                    min = ill;
                }
                list.add(ill);
            }
            dataMap.put(column, list);
            maxAndMinValues.put(column, Lists.newArrayList(max, min > 0 ? 0 : min));
        } else if(type == ColumnType.GPS_SPEED || type == ColumnType.GPS_BEARING) {
            List<Integer> list = Lists.newArrayList();
            int factor = 10;
            int max = 0;
            for(Object data : datas) {
                if(data == null) {
                    list.add(null);
                    continue;
                }
                float fll = (Float)data;
                if(fll < 0) {
                    list.add(null);
                    continue;
                }
                int ill = (int) (fll * factor);
                if (ill > max) {
                    max = ill;
                }
                list.add(ill);
            }
            dataMap.put(column, list);
            maxAndMinValues.put(column, Lists.newArrayList(max, 0));
        }
        if (columnConfig.getEncodeType().needMaxAndMin()) {
            if (type == ColumnType.BOOL) {
                maxAndMinValues.put(column, Lists.newArrayList(1, 0));
            } else if (type == ColumnType.LONG ||  type == ColumnType.UNSIGNED_LONG) {
                Long max = Long.MIN_VALUE;
                Long min = Long.MAX_VALUE;
                for (Object data : datas) {
                    if (data == null) {
                        continue;
                    }
                    long v = Long.parseLong(data.toString());
                    if (v > max) {
                        max = v;
                    }
                    if (v < min) {
                        min = v;
                    }
                }
                maxAndMinValues.put(column, Lists.newArrayList(max, min > 0 ? 0 : min));
            } else if(type == ColumnType.BYTE || type == ColumnType.INT || type == ColumnType.UNSIGNED_INT || type == ColumnType.SHORT) {
                Integer max = Integer.MIN_VALUE;
                Integer min = Integer.MAX_VALUE;
                for (Object data : datas) {
                    if (data == null) {
                        continue;
                    }
                    Integer v = Integer.parseInt(data.toString());
                    if (v > max) {
                        max = v;
                    }
                    if (v < min) {
                        min = v;
                    }
                }
                maxAndMinValues.put(column, Lists.newArrayList(max, min > 0 ? 0 : min));
            }
        }
    }

    private ValuesWriter getValuesWriter(Column column) {
        ColumnConfig columnConfig = columnConfigs.get(column);
        if(columnConfig == null) {
            throw new ZebraEncodingException("Not select column for builder");
        }
        if(columnConfig.getEncodeType() == EncodeType.bearing_encoding) {
            return new GpsBearingValuesWriter(128);
        }
        if(columnConfig.getEncodeType() == EncodeType.delta) {
            if(columnConfig.getColumnType() == ColumnType.LONG || columnConfig.getColumnType() == ColumnType.UNSIGNED_LONG) {
                return new DeltaLongBitPackingValuesWriter(32, 1, 128);
            }
            return new DeltaIntBitPackingValuesWriter(32, 1, 128);
        }
        if(columnConfig.getEncodeType() == EncodeType.delta_prefix_int) {
            List<?> maxMin = maxAndMinValues.get(column);
            if(CollectionUtils.isEmpty(maxMin)) {
                throw new ZebraEncodingException("Not set processData column =" + column.name());
            }
            int max = (Integer) maxMin.get(0);
            int min = (Integer) maxMin.get(1);
            return new DeltaPrefixIntegerWriter(max / 10000, min / 10000, 4, 128);
        }
        if(columnConfig.getEncodeType() == EncodeType.rle) {
            List<?> maxMin = maxAndMinValues.get(column);
            if(CollectionUtils.isEmpty(maxMin)) {
                throw new ZebraEncodingException("Not set processData column =" + column.name());
            }
            if(columnConfig.getColumnType() == ColumnType.LONG || columnConfig.getColumnType() == ColumnType.UNSIGNED_LONG) {
                long max = (Long) maxMin.get(0);
                long min = (Long) maxMin.get(1);
                return new RunLengthBitPackingHybridValuesWriter(max, min, 128);
            }
            int max = (Integer) maxMin.get(0);
            int min = (Integer) maxMin.get(1);
            return new RunLengthBitPackingHybridValuesWriter(max, min, 128);
        }
        if(columnConfig.getEncodeType() == EncodeType.speed_encoding) {
            return new GpsSpeedValuesWriter(128);
        }
        throw new ZebraEncodingException("Not support this encoding column =" + column.name() + ",encodingType=" + columnConfig.getColumnType());
    }

}
