package com.icongtai.zebra.encoding.format;

import com.icongtai.zebra.encoding.bytes.Binary;

/**
 *
 */
public enum ColumnType {
    BYTE(0, Byte.class),
    SHORT(1, Short.class),
    INT(2, Integer.class),
    LONG(3, Long.class),
    BOOL(4, Boolean.class),
    FLOAT(5, Float.class),
    DOUBLE(6, Double.class),
    STRING(7, Binary.class),
    /**
     * 无符号整数int
     */
    UNSIGNED_INT(8, Integer.class),

    /**
     * 无符号整数long
     */
    UNSIGNED_LONG(9, Long.class),

//    特殊类型
    /**
     * gps经纬度
     */
    GPS_LL(10, Integer.class),
    /**
     * gps速度
     */
    GPS_SPEED(11, Integer.class),
    /**
     * gps方向
     */
    GPS_BEARING(12, Integer.class);

    private int type;

    private Class<?> clazz;

    ColumnType(int type, Class<?> clazz) {
        this.type = type;
        this.clazz = clazz;
    }

    public int getType() {
        return type;
    }

    public Class getClazz() {
        return clazz;
    }

    }