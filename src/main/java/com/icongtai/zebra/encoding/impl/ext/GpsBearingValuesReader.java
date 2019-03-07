package com.icongtai.zebra.encoding.impl.ext;

import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesReader;

import java.io.IOException;

/**
 * {@link GpsBearingValuesWriter}
 */
public class GpsBearingValuesReader extends ValuesReader {

    private ValuesReader deltaReader;

    private DeltaIntBitPackingValuesReader rleIndexReader;

    private RunLengthBitPackingHybridValuesReader rleReader;

    private int readCount;

    private int rleIndex = Integer.MAX_VALUE;

    private int rleIndexCount;

    public GpsBearingValuesReader() {
        this.deltaReader = new DeltaGpsBearingValuesReader();
        this.rleIndexReader = new DeltaIntBitPackingValuesReader();
        this.rleReader = new RunLengthBitPackingHybridValuesReader(false);
    }

    @Override
    public void init(int valueCount, byte[] buf, int offset) throws IOException {
        deltaReader.init(valueCount, buf, offset);

        int next = deltaReader.getNextOffset();
        rleIndexReader.init(valueCount, buf, next);

        next = rleIndexReader.getNextOffset();
        rleReader.init(valueCount, buf, next);

        if (rleIndexReader.getTotalValueCount() > rleIndexCount) {
            rleIndex = rleIndexReader.readInteger();
            ++rleIndexCount;
        }
    }

    @Override
    public int readInteger() {
        int v = 0;
        if (readCount >= rleIndex) {
            v = rleReader.readInteger();
            if (rleReader.getCurrentCount() == 0) {
                if (rleIndexReader.getTotalValueCount() > rleIndexCount) {
                    rleIndex = rleIndexReader.readInteger();
                    ++rleIndexCount;
                } else {
                    rleIndex = Integer.MAX_VALUE;
                }
            }
        } else {
            v = deltaReader.readInteger();
        }

        ++readCount;
        return v;
    }

    @Override
    public void skip() {
        readInteger();
    }

    @Override
    public int getNextOffset() {
        return rleReader.getNextOffset();
    }
}
