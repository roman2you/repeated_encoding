package com.icongtai.zebra.encoding.impl.ext;

import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesWriter;

/**
 *  Write bearing integers with delta encoding and rle encoding
 * The format is as follows:
 * <p/>
 * <pre>
 *   {@code
 *     gps-bearing encoding: <delta-block> <rle-index-block> <rle-block>
 *     delta-block := deltaBearingPacking  the block for non continue bearing
 *     rle-index-block := deltaIntBitPacking the block for rle relative offset list
 *     rle-block := rle  the block for continue bearing
 *   }
 * </pre>
 *
 */
public class GpsBearingValuesWriter extends ValuesWriter {

    private ValuesWriter deltaWriter;

    private ValuesWriter rleIndexWriter;

    private ValuesWriter rleWriter;

    private int previousValue;

    private int writeCount;

    private boolean startRle;

    public GpsBearingValuesWriter(int initSlabSize) {
        deltaWriter = new DeltaGpsBearingValuesWriter(32, 1, initSlabSize);
        rleIndexWriter = new DeltaIntBitPackingValuesWriter(32, 1, initSlabSize);
        rleWriter = new RunLengthBitPackingHybridValuesWriter(3600, 0, initSlabSize);
    }

    @Override
    public void writeInteger(int v) {
        ++writeCount;
        if(writeCount == 1) {
            previousValue = v;
            return;
        }
        if(previousValue == v) {
            if(!startRle ) {
                rleIndexWriter.writeInteger(writeCount - 2);
                startRle = true;
            }
            rleWriter.writeInteger(previousValue);
        } else {
            if(startRle) {
                rleWriter.writeInteger(previousValue);
                startRle = false;
            } else {
                deltaWriter.writeInteger(previousValue);
            }
        }
        previousValue = v;
    }

    @Override
    public long getBufferedSize() {
        return rleIndexWriter.getBufferedSize() + deltaWriter.getBufferedSize() + rleWriter.getBufferedSize();
    }

    @Override
    public BytesInput getBytes() {
        if(startRle) {
            rleIndexWriter.writeInteger(previousValue);
        } else {
            deltaWriter.writeInteger(previousValue);
        }
        return BytesInput.concat(deltaWriter.getBytes(), rleIndexWriter.getBytes(), rleWriter.getBytes());
    }

    @Override
    public EncodeType getEncoding() {
        return EncodeType.bearing_encoding;
    }

    @Override
    public void reset() {
        rleIndexWriter.reset();
        deltaWriter.reset();
        rleWriter.reset();
    }

    @Override
    public long getAllocatedSize() {
        return deltaWriter.getAllocatedSize() + rleIndexWriter.getAllocatedSize() + rleWriter.getAllocatedSize();
    }

    @Override
    public String memUsageString(String prefix) {
        return String.format("%s GpsBearing %d bytes", prefix, getAllocatedSize());
    }
}
