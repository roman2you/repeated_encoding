package com.icongtai.zebra.encoding.impl.ext;

import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter;

/**
 *
 */
class DeltaGpsBearingValuesWriter extends DeltaIntBitPackingValuesWriter {

    public DeltaGpsBearingValuesWriter(int initSlabSize) {
        this(DEFAULT_NUM_BLOCK_VALUES, DEFAULT_NUM_MINIBLOCKS, initSlabSize);
    }

    public DeltaGpsBearingValuesWriter(int blockSizeInValues, int miniBlockNum, int initSlabSize) {
        super(blockSizeInValues, miniBlockNum, initSlabSize);
    }

    @Override
    public void writeInteger(int v) {
        totalValueCount++;

        if (totalValueCount == 1) {
            firstValue = v;
            previousValue = firstValue;
            return;
        }

        int delta = v - previousValue;//calculate delta
        if(delta > 1800) {
            delta = -(3600 - delta);
        } else if(delta < -1800) {
            delta = 3600 + delta;
        }

        previousValue = v;

        deltaBlockBuffer[deltaValuesToFlush++] = delta;

        if (delta < minDeltaInCurrentBlock) {
            minDeltaInCurrentBlock = delta;
        }

        if (flushBlockCondition()) {
            flushBlockBuffer();
        }
    }



    @Override
    public EncodeType getEncoding() {
        return EncodeType.delta_bearing;
    }


    @Override
    public String memUsageString(String prefix) {
        return String.format("%s Delta_Bearing %d bytes", prefix, getAllocatedSize());
    }
}
