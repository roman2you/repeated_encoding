package com.icongtai.zebra.encoding.format;

/**
 * Created by robin on 16/9/14.
 */
public enum EncodeType {
    /**
     * java基础类型通用编码
     * {@link com.icongtai.zebra.encoding.impl.plain.PlainValuesWriter}
     */
    plain(0),
    /**
     * 数字类型变长编码，目前只支持Int和Long
     * {@link com.icongtai.zebra.encoding.impl.varplain.VarPlainValuesWriter}
     */
    var_plain(1),
    /**
     * 适合自增id或timestamp的编码 </br>
     * {@link com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter}
     */
    delta(2),
    /**
     * delta编码for string；</br>
     * 编码格式：{@link com.icongtai.zebra.encoding.impl.deltastrings.DeltaByteArrayWriter}
     */
    delta_byte_array(3),
    /**
     * ByteArray对length+bytes[]进行组合编码
     * {@link com.icongtai.zebra.encoding.impl.deltalengthbytearray.DeltaLengthByteArrayValuesWriter}
     */
    delta_length_byte_array(4),

    /**
     * 对方向0-360角度的特性delta进行编码
     * {@link com.icongtai.zebra.encoding.impl.ext.DeltaGpsBearingValuesWriter}
     */
    delta_bearing(5),
    /**
     * 适合大量重叠的编码</br>
     * {@link com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesWriter}
     */
    rle(6),
    /**
     * bit编码
     * {@link com.icongtai.zebra.encoding.impl.bitpacking.BitPackingValuesWriter}
     */
    bit_packing(7),
    /**
     * gps经纬度编码，val = (val * 100W).toInt().toString();
     * int v1 = 数字后3位，delta编码
     * int v2 = 剩余数字，rle编码
     */
    delta_prefix_int(8),
    /**
     *
     * gps速度编码，速度支持1个小数点的精度，重点问题是speed会忽然漂0，会出现连续以端是0的速度。
     * 负数的表示null,遇到速度15码以上忽然变成0，或者0忽然到15码以上，这个时候的连续0值，数字采用var_plain编码
     * 编码格式delta+var_plain+delta
     *
     */
    speed_encoding(9),
    /**
     * gps方向编码，重点的问题是速度是0-360,所以delta的值需要特殊处理0度左右的差值
     * 速度支持1个小数点的精度，负数的表示null，
     * 编码格式delta
     */
    bearing_encoding(10),
    /**
     * 字典编码；字典id替换字典value
     */
    plan_dictionary(11);

    private int type;

    EncodeType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public boolean needMaxAndMin() {
        if(type == 6 || type == 7 || type == 8) {
            return true;
        }
        return false;
    }
}