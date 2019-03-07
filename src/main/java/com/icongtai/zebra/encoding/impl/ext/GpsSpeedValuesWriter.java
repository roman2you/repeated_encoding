package com.icongtai.zebra.encoding.impl.ext;

import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter;

/**
 * Write speed integers  with delta encoding
 * The format is as follows:
 * <p/>
 * <pre>
 *   {@code
 *     gps-speed encoding: <zero-index-block> <zero-length-block> <nonZero-block>
 *     zero-index-block := deltaIntBitPacking
 *     zero-length-block := deltaIntBitPacking
 *     nonZero-block := deltaIntBitPacking
 *   }
 * </pre>
 *
 */
public class GpsSpeedValuesWriter extends ValuesWriter {

    private ValuesWriter deltaValuesWriter;

    private ValuesWriter zeroValuesWriter;

    private ValuesWriter zeroIndexValuesWriter;


    /**
     * 0所在的索引位置
     */
    protected int zeroValuesIndex = 0;

    /**
     * 0值的单元长度
     */
    private int zeroValuesToFlush = 0;

    /**
     * 索引标记位
     */
    private int index;


    public GpsSpeedValuesWriter(int initSlabSize) {
        deltaValuesWriter = new DeltaIntBitPackingValuesWriter(32, 1, initSlabSize);
        zeroValuesWriter = new DeltaIntBitPackingValuesWriter(32, 1, initSlabSize);
        zeroIndexValuesWriter = new DeltaIntBitPackingValuesWriter(32, 1, initSlabSize);
    }

    @Override
    public long getBufferedSize() {
        return deltaValuesWriter.getBufferedSize() + zeroValuesWriter.getBufferedSize() + zeroIndexValuesWriter.getBufferedSize();
    }

    @Override
    public void writeInteger(int v) {
        if (v == 0) {
            ++zeroValuesToFlush;
            if (zeroValuesIndex == 0) {
                zeroValuesIndex = index;
            }
        } else {
            if (zeroValuesToFlush > 0) {
                flushZero();
                zeroValuesToFlush = 0;
                zeroValuesIndex = 0;
            }
            deltaValuesWriter.writeInteger(v);
        }
        ++index;
    }

    private void flushZero() {
        zeroIndexValuesWriter.writeInteger(zeroValuesIndex);
        zeroValuesWriter.writeInteger(zeroValuesToFlush);
    }


    /**
     * getBytes will trigger flushing block buffer, DO NOT write after getBytes() is called without calling reset()
     *
     * @return
     */
    @Override
    public BytesInput getBytes() {
        if(zeroValuesIndex > 0) {
            flushZero();
        }
        //The Page Header should include: blockSizeInValues, numberOfMiniBlocks, totalValueCount
        return BytesInput.concat(
                zeroIndexValuesWriter.getBytes(),
                zeroValuesWriter.getBytes(),
                deltaValuesWriter.getBytes());
    }

    @Override
    public EncodeType getEncoding() {
        return EncodeType.speed_encoding;
    }

    @Override
    public void reset() {
        this.zeroIndexValuesWriter.reset();
        this.zeroValuesWriter.reset();
        this.deltaValuesWriter.reset();
        this.zeroValuesIndex = 0;
        this.zeroValuesToFlush = 0;
        this.index = 0;
    }

    @Override
    public long getAllocatedSize() {
        return zeroIndexValuesWriter.getAllocatedSize() + zeroValuesWriter.getAllocatedSize() + deltaValuesWriter.getAllocatedSize();
    }

    @Override
    public String memUsageString(String prefix) {
        return String.format("%s Speed_Encoder %d bytes", prefix, getAllocatedSize());
    }
}
