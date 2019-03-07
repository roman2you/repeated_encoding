package com.icongtai.zebra.encoding.impl.rle;

import com.google.common.base.Preconditions;
import com.icongtai.zebra.encoding.bitpacking.BytePackerForLong;
import com.icongtai.zebra.encoding.bitpacking.Packer;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.bytes.CapacityByteArrayOutputStream;
import com.icongtai.zebra.encoding.common.Log;

import java.io.IOException;

class RunLengthBitPackingForLongHybridEncoder extends RunLengthBitPackingHybridEncoder {

    public static final Log LOG = Log.getLog(RunLengthBitPackingForLongHybridEncoder.class);

    private final BytePackerForLong packer;

    /**
     * Previous value written, used to detect repeated values
     */
    private long previousValue;

    /**
     * We buffer 8 values at a time, and either bit pack them
     * or discard them after writing a rle-run
     */
    private long[] bufferedValues;
    private int numBufferedValues;

    public RunLengthBitPackingForLongHybridEncoder(int bitWidth, boolean signed, int initialCapacity) {
        LOG.debug(String.format("Encoding: RunLengthBitPackingForLongHybridEncoder with "
                + "bithWidth: %d initialCapacity %d", bitWidth, initialCapacity));

        Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");

        this.bitWidth = bitWidth;
        this.signed = signed;
        this.baos = new CapacityByteArrayOutputStream(initialCapacity);
        //fit byte，0位表示符号标示,signed=1，1-6位代表bitWidth，bitWidth max value=63
        int head = bitWidth << 1;
        this.baos.write(signed ? head | 1 : head);
        this.packBuffer = new byte[bitWidth];
        this.bufferedValues = new long[8];
        this.packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
        reset(false);
    }

    protected void reset(boolean resetBaos) {
        if (resetBaos) {
            this.baos.reset();
        }
        this.previousValue = 0;
        this.numBufferedValues = 0;
        this.repeatCount = 0;
        this.bitPackedGroupCount = 0;
        this.bitPackedRunHeaderPointer = -1;
        this.toBytesCalled = false;
    }

    public void writeLong(long value) throws IOException {
        if (signed) {
            value = BytesUtils.zigzagLong(value);
        }
        if (value == previousValue) {
            // keep track of how many times we've seen this value
            // consecutively
            ++repeatCount;

            if (repeatCount >= 8) {
                // we've seen this at least 8 times, we're
                // certainly going to write an rle-run,
                // so just keep on counting repeats for now
                return;
            }
        } else {
            // This is a new value, check if it signals the end of
            // an rle-run
            if (repeatCount >= 8) {
                // it does! write an rle-run
                writeRleRun();
            }

            // this is a new value so we've only seen it once
            repeatCount = 1;
            // start tracking this value for repeats
            previousValue = value;
        }

        // We have not seen enough repeats to justify an rle-run yet,
        // so buffer this value in case we decide to write a bit-packed-run
        bufferedValues[numBufferedValues] = value;
        ++numBufferedValues;

        if (numBufferedValues == 8) {
            // we've encountered less than 8 repeated values, so
            // either start a new bit-packed-run or append to the
            // current bit-packed-run
            writeOrAppendBitPackedRun();
        }
    }

    private void writeOrAppendBitPackedRun() throws IOException {
        if (bitPackedGroupCount >= 63) {
            // we've packed as many values as we can for this run,
            // end it and start a new one
            endPreviousBitPackedRun();
        }

        if (bitPackedRunHeaderPointer == -1) {
            // this is a new bit-packed-run, allocate a byte for the header
            // and keep a "pointer" to it so that it can be mutated later
            baos.write(0); // write a sentinel value
            bitPackedRunHeaderPointer = baos.getCurrentIndex();
        }

        packer.pack8Values(bufferedValues, 0, packBuffer, 0);
        baos.write(packBuffer);

        // empty the buffer, they've all been written
        numBufferedValues = 0;

        // clear the repeat count, as some repeated values
        // may have just been bit packed into this run
        repeatCount = 0;

        ++bitPackedGroupCount;
    }

    private void writeRleRun() throws IOException {
        // we may have been working on a bit-packed-run
        // so close that run if it exists before writing this
        // rle-run
        endPreviousBitPackedRun();

        // write the rle-header (lsb of 0 signifies a rle run)
        BytesUtils.writeUnsignedVarInt(repeatCount << 1, baos);
        // write the repeated-value
        BytesUtils.writeLongLittleEndianPaddedOnBitWidth(baos, previousValue, bitWidth);

        // reset the repeat count
        repeatCount = 0;

        // throw away all the buffered values, they were just repeats and they've been written
        numBufferedValues = 0;
    }


    public BytesInput toBytes() throws IOException {
        Preconditions.checkArgument(!toBytesCalled,
                "You cannot call toBytes() more than once without calling reset()");

        // write anything that is buffered / queued up for an rle-run
        if (repeatCount >= 8) {
            writeRleRun();
        } else if (numBufferedValues > 0) {
            for (int i = numBufferedValues; i < 8; i++) {
                bufferedValues[i] = 0;
            }
            writeOrAppendBitPackedRun();
            endPreviousBitPackedRun();
        } else {
            endPreviousBitPackedRun();
        }

        toBytesCalled = true;
        return BytesInput.from(baos);
    }



}