package com.icongtai.zebra.encoding.impl.rle;


import com.google.common.base.Preconditions;
import com.icongtai.zebra.encoding.bitpacking.BytePackerForLong;
import com.icongtai.zebra.encoding.bitpacking.Packer;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

class RunLengthBitPackingHybridForLongDecoder extends RunLengthBitPackingHybridDecoder {

    public static final Log LOG = Log.getLog(RunLengthBitPackingHybridForLongDecoder.class);

    private final BytePackerForLong packer;

    private int currentCount;
    private long currentValue;
    private long[] currentBuffer;


    RunLengthBitPackingHybridForLongDecoder(InputStream in) {
        this.in = in;
        int head = 0;
        try {
            head = in.read();
        } catch (IOException e) {
            throw new ZebraDecodingException("not data for decode");
        }
        if ((head & 1) == 1) {
            this.signed = true;
        } else {
            this.signed = false;
        }
        head = head & 0x7F;
        this.bitWidth = head >>> 1;
        LOG.debug("decoding bitWidth " + bitWidth);
        Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
        this.packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
    }

    public long readLong() throws IOException {
        if (currentCount == 0) {
            readNext();
        }
        --currentCount;
        long result;
        switch (mode) {
            case RLE:
                result = currentValue;
                break;
            case PACKED:
                result = currentBuffer[currentBuffer.length - 1 - currentCount];
                break;
            default:
                throw new ZebraDecodingException("not a valid mode " + mode);
        }
        if (signed) {
            return BytesUtils.decodeZigZagLong(result);
        }
        return result;
    }

    @Override
    public int getCurrentCount() {
        return currentCount;
    }

    private void readNext() throws IOException {
        Preconditions.checkArgument(in.available() > 0, "Reading past RLE/BitPacking stream.");
        final int header = BytesUtils.readUnsignedVarInt(in);
        mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
        switch (mode) {
            case RLE:
                currentCount = header >>> 1;
                LOG.debug("reading " + currentCount + " values RLE");
                currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
                break;
            case PACKED:
                int numGroups = header >>> 1;
                currentCount = numGroups * 8;
                LOG.debug("reading " + currentCount + " values BIT PACKED");
                currentBuffer = new long[currentCount]; // TODO: reuse a buffer
                byte[] bytes = new byte[numGroups * bitWidth];
                // At the end of the file RLE data though, there might not be that many bytes left.
                int bytesToRead = (int) Math.ceil(currentCount * bitWidth / 8.0);
                bytesToRead = Math.min(bytesToRead, in.available());
                new DataInputStream(in).readFully(bytes, 0, bytesToRead);
                for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex += bitWidth) {
                    packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
                }
                break;
            default:
                throw new ZebraDecodingException("not a valid mode " + mode);
        }
    }

}
