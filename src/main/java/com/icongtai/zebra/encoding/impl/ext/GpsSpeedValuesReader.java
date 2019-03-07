package com.icongtai.zebra.encoding.impl.ext;

import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesReader;

import java.io.IOException;

/**
 * {@link GpsBearingValuesWriter}
 */
public class GpsSpeedValuesReader extends ValuesReader {

    /**
     * deltaReader for non zero
     */
    private DeltaIntBitPackingValuesReader deltaReader;

    /**
     * zero Index buffer
     */
    private DeltaIntBitPackingValuesReader zeroIndexReader;

    /**
     * zero length buffer
     */
    private DeltaIntBitPackingValuesReader zeroReader;

    private int currentZeroIndex = -1;

    private int currentZeroLength = -1;

    private int zeroIndex;

    private int readIndex;

    private int zeroCount;

    public GpsSpeedValuesReader() {
        deltaReader = new DeltaIntBitPackingValuesReader();
        zeroIndexReader = new DeltaIntBitPackingValuesReader();
        zeroReader = new DeltaIntBitPackingValuesReader();
    }

    @Override
    public void init(int valueCount, byte[] buf, int offset) throws IOException {
        zeroIndexReader.init(valueCount, buf, offset);
        int next = zeroIndexReader.getNextOffset();
        zeroReader.init(valueCount, buf, next);
        next = zeroReader.getNextOffset();
        deltaReader.init(valueCount, buf, next);

        zeroCount = zeroIndexReader.getTotalValueCount();
        readZero();
    }

    private void readZero() {
        if (zeroIndex < zeroCount) {
            currentZeroIndex = zeroIndexReader.readInteger();
            currentZeroLength = zeroReader.readInteger();
            ++zeroIndex;
        } else {
            currentZeroIndex = -1;
            currentZeroLength = -1;
        }
    }

    @Override
    public int readInteger() {
        if (currentZeroIndex > 0 && readIndex >= currentZeroIndex && readIndex < currentZeroIndex + currentZeroLength) {
            if (readIndex == currentZeroIndex + currentZeroLength - 1) {
                readZero();
            }
            ++readIndex;
            return 0;
        }
        ++readIndex;
        return deltaReader.readInteger();
    }

    @Override
    public void skip() {
        readInteger();
    }

    @Override
    public int getNextOffset() {
        return deltaReader.getNextOffset();
    }
}
