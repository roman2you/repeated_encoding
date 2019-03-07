package com.icongtai.zebra.encoding.impl.rle;

import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.CapacityByteArrayOutputStream;
import com.icongtai.zebra.encoding.common.Log;

import java.io.IOException;


/**
 * Encodes values using a combination of run length encoding and bit packing,
 * according to the following grammar:
 * <p/>
 * <pre>
 * {@code
 * rle-bit-packed-hybrid: <length> <head> <encoded-data>
 * length := length of the <encoded-data> in bytes stored as 4 bytes little endian
 * head := bitWidth << 1 | 0(unsigned) or 1(signed)
 * encoded-data := <run>*
 * run := <bit-packed-run> | <rle-run>
 * bit-packed-run := <bit-packed-header> <bit-packed-values>
 * bit-packed-header := varInt-encode(<bit-pack-count> << 1 | 1)1表示是BIT_PACKING模式
 * // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
 * bit-pack-count := (number of values in this run) / 8
 * bit-packed-values :=  bit packed back to back, from LSB to MSB
 * rle-run := <rle-header> <repeated-value>
 * rle-header := varint-encode( (number of times repeated) << 1 | 0)0表示是RLE模式
 * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
 * }
 * </pre>
 * NOTE: this class is only responsible for creating and returning the {@code <encoded-data>}
 * portion of the above grammar. The {@code <length>} portion is done by
 * <p/>
 */
abstract public class RunLengthBitPackingHybridEncoder {
    private static final Log LOG = Log.getLog(RunLengthBitPackingHybridEncoder.class);

    protected CapacityByteArrayOutputStream baos;

    /**
     * The bit width used for bit-packing and for writing
     * the repeated-value
     */
    protected int bitWidth;

    /**
     * the values has contains signed number
     */
    protected boolean signed;

    /**
     * Values that are bit packed 8 at at a time are packed into this
     * buffer, which is then written to baos
     */
    protected byte[] packBuffer;


    /**
     * How many times a value has been repeated
     */
    protected int repeatCount;

    /**
     * How many groups of 8 values have been written
     * to the current bit-packed-run
     */
    protected int bitPackedGroupCount;

    /**
     * A "pointer" to a single byte in baos,
     * which we use as our bit-packed-header. It's really
     * the logical index of the byte in baos.
     * <p/>
     * We are only using one byte for this header,
     * which limits us to writing 504 values per bit-packed-run.
     * <p/>
     * MSB must be 0 for varint encoding, LSB must be 1 to signify
     * that this is a bit-packed-header leaves 6 bits to write the
     * number of 8-groups -> (2^6 - 1) * 8 = 504
     */
    protected long bitPackedRunHeaderPointer;

    protected boolean toBytesCalled;

    /**
     * If we are currently writing a bit-packed-run, update the
     * bit-packed-header and consider this run to be over
     * <p/>
     * does nothing if we're not currently writing a bit-packed run
     */
    protected void endPreviousBitPackedRun() {
        if (bitPackedRunHeaderPointer == -1) {
            // we're not currently in a bit-packed-run
            return;
        }

        // create bit-packed-header, which needs to fit in 1 byte
        byte bitPackHeader = (byte) ((bitPackedGroupCount << 1) | 1);

        // update this byte
        baos.setByte(bitPackedRunHeaderPointer, bitPackHeader);

        // mark that this run is over
        bitPackedRunHeaderPointer = -1;

        // reset the number of groups
        bitPackedGroupCount = 0;
    }

    protected  void reset(boolean resetBaos){
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Reset this encoder for re-use
     */
    public void reset() {
        reset(true);
    }

    public long getBufferedSize() {
        return baos.size();
    }

    public long getAllocatedSize() {
        return baos.getCapacity();
    }

    /**
     * @param v the value to encode
     */
    public void writeInt(int v) throws IOException {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * @param v the value to encode
     */
    public void writeLong(long v) throws IOException {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public BytesInput toBytes() throws IOException{
        throw new UnsupportedOperationException(getClass().getName());
    }

    public static RunLengthBitPackingHybridEncoder newIntEncoder(int bitWidth, boolean signed, int initCapacity) {
        return new RunLengthBitPackingForIntHybridEncoder(bitWidth, signed, initCapacity);
    }

    public static RunLengthBitPackingHybridEncoder newLongEncoder(int bitWidth, boolean signed, int initCapacity) {
        return new RunLengthBitPackingForLongHybridEncoder(bitWidth, signed, initCapacity);
    }


    public static void main(String[] args) throws Exception {
        RunLengthBitPackingHybridEncoder encoder = newIntEncoder(3, false, 16);
        for (int i = 0; i < 23; i++) {
            encoder.writeInt(6);
        }
        encoder.toBytes();
    }
}